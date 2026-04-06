from flask import (
    Blueprint,
    render_template,
    session,
    redirect,
    url_for,
    current_app,
    request,
    jsonify,
    Response,
)
import os
import shutil
import time
import json
import base64
import threading
import logging

from plexapi.myplex import MyPlexAccount

from ..services.ftp_download_service import register_post_download_hook
from ..services.mkvtoolnix_service import (
    get_mkvtoolnix_binaries,
    extract_languages,
    get_mkv_status,
    find_source_path,
    collect_video_files,
    build_destination_path,
    remux_file,
    load_mkv_languages,
    MKVCancelledError,
)

from ..services.storage_service import get_disk_usage_info
from ..services.plex_service import connect_to_server

import re
from typing import Optional, Tuple


def _normalize_match_title(title: str) -> str:
    t = (title or "").strip().lower()
    t = t.replace(".", " ").replace("_", " ").replace("-", " ")
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _clean_release_name_for_guess(name: str) -> Tuple[str, Optional[int]]:
    """
    Ex:
    La.Candidate.2023.S01.FRENCH.1080p.WEB.H264-TFA
    -> ("La Candidate", 2023)
    """
    raw = (name or "").strip()
    raw = os.path.splitext(os.path.basename(raw))[0]
    txt = raw.replace(".", " ").replace("_", " ").replace("-", " ")

    year = None
    m = re.search(r"\b(19\d{2}|20\d{2})\b", txt)
    if m:
        try:
            year = int(m.group(1))
        except Exception:
            year = None

    stop = re.search(
        r"(?i)\b("
        r"S\d{1,2}E\d{1,2}|S\d{1,2}|Season\s*\d+|Saison\s*\d+|"
        r"2160p|1080p|720p|480p|WEB[- ]?DL|WEBRip|BluRay|BRRip|DVDRip|HDRip|REMUX|"
        r"x264|x265|h264|h265|HEVC|AAC|AC3|DTS|DDP5\.1|ATMOS|"
        r"FRENCH|TRUEFRENCH|VFF|VFQ|MULTI|SUBFRENCH|PROPER|REPACK|COMPLETE"
        r")\b",
        txt,
    )
    if stop:
        txt = txt[:stop.start()].strip()

    if year:
        txt = re.sub(rf"\b{year}\b", " ", txt)

    txt = re.sub(r"\s+", " ", txt).strip(" -._")
    return txt, year


def _get_local_plex_server(plex_token):
    if not plex_token:
        return None

    try:
        account = MyPlexAccount(token=plex_token)
        my_server_res = next(
            (
                s for s in account.resources()
                if s.provides == "server" and getattr(s, "owned", False)
            ),
            None
        )
        if not my_server_res:
            return None

        return connect_to_server(my_server_res, plex_token, prefer_local=True)
    except Exception as e:
        current_app.logger.warning(f"[MKV UI] Impossible de connecter Plex local: {e}")
        return None


def _find_best_plex_match(server, guessed_title: str, guessed_year: Optional[int], media_type: str):
    """
    media_type: 'show' ou 'movie'
    Retourne (title, year) ou ("", None)
    """
    if not server or not guessed_title:
        return "", None

    guess_norm = _normalize_match_title(guessed_title)
    best = None
    best_score = -1

    try:
        for section in server.library.sections():
            if media_type == "show" and section.type != "show":
                continue
            if media_type == "movie" and section.type != "movie":
                continue

            try:
                candidates = section.search(title=guessed_title)
            except Exception:
                continue

            for item in candidates or []:
                title = getattr(item, "title", "") or ""
                year = getattr(item, "year", None)

                item_norm = _normalize_match_title(title)
                score = 0

                if item_norm == guess_norm:
                    score += 100
                elif guess_norm and guess_norm in item_norm:
                    score += 70
                elif item_norm and item_norm in guess_norm:
                    score += 50

                if guessed_year and year:
                    if int(year) == int(guessed_year):
                        score += 30
                    else:
                        score -= 10

                if score > best_score:
                    best_score = score
                    best = (title, year)

    except Exception as e:
        current_app.logger.warning(f"[MKV UI] Recherche Plex échouée: {e}")
        return "", None

    if best and best_score >= 70:
        return best

    return "", None


def _resolve_plex_show_name(server, rel_folder: str) -> Tuple[str, Optional[int]]:
    """
    rel_folder ex:
    'Serie TV/La.Candidate.2023.S01.FRENCH.1080p.WEB.H264-TFA'
    """
    parts = [p for p in rel_folder.split(os.sep) if p]
    source_name = parts[-1] if parts else rel_folder

    guessed_title, guessed_year = _clean_release_name_for_guess(source_name)
    plex_title, plex_year = _find_best_plex_match(server, guessed_title, guessed_year, "show")

    if plex_title:
        return plex_title, plex_year

    return "", None

mkv_bp = Blueprint("mkv", __name__)

IGNORED_DIRS = {".grab", ".Recycle.Bin", "$RECYCLE.BIN"}

from collections import deque

progress_data: dict = {}
progress_lock = threading.Lock()

cancel_flags: dict = {}

logger = logging.getLogger(__name__)
active_processes: dict = {}

job_queue: deque = deque(maxlen=500)
queue_worker_started = False

_JOB_TTL_SECONDS = 3600  # purge jobs terminés après 1h


def _purge_old_mkv_jobs():
    """Supprime progress_data des jobs terminés > TTL. Doit être appelé sous progress_lock."""
    now = time.time()
    terminal = {"done", "error", "cancelled"}
    to_delete = [
        jid for jid, state in progress_data.items()
        if state.get("status") in terminal
        and now - state.get("finished_at", now) > _JOB_TTL_SECONDS
    ]
    for jid in to_delete:
        del progress_data[jid]

def _normalize_label(text: str) -> str:
    return "".join(ch.lower() for ch in (text or "").strip() if ch.isalnum())


def _is_series_category_name(category: str) -> bool:
    low = (category or "").strip().lower()
    return any(k in low for k in ("série", "serie", "show", "séries", "series", "shows", "anime"))


def _is_series_with_server(server, category_name: str) -> bool:
    """Résout le type de catégorie en utilisant une connexion Plex déjà établie."""
    if not server:
        return _is_series_category_name(category_name)
    target = _normalize_label(category_name)
    try:
        for sec in server.library.sections():
            if _normalize_label(sec.title) == target:
                return sec.type == "show"
    except Exception:
        pass
    return _is_series_category_name(category_name)


def resolve_category_is_series(plex_token, category_name):
    if not plex_token or not category_name:
        return _is_series_category_name(category_name)

    try:
        account = MyPlexAccount(token=plex_token)
        my_server_res = next(
            (s for s in account.resources() if s.provides == "server" and getattr(s, "owned", False)),
            None
        )
        my_server = connect_to_server(my_server_res, plex_token, prefer_local=True)

        if not my_server:
            return _is_series_category_name(category_name)

        target = _normalize_label(category_name)

        for sec in my_server.library.sections():
            if _normalize_label(sec.title) == target:
                return sec.type == "show"

        return _is_series_category_name(category_name)

    except Exception:
        return _is_series_category_name(category_name)


def _set_job_state(job_id, **updates):
    with progress_lock:
        if job_id not in progress_data:
            progress_data[job_id] = {}
        progress_data[job_id].update(updates)


def _enqueue_job(job):
    with progress_lock:
        _purge_old_mkv_jobs()
        job_queue.append(job)


def _pop_next_pending_job():
    with progress_lock:
        while job_queue:
            job = job_queue.popleft()  # O(1) vs list.pop(0) O(n)
            job_id = job["job_id"]

            state = progress_data.get(job_id, {})
            if state.get("cancelled"):
                continue
            if state.get("status") == "cancelled":
                continue

            return job
    return None


def _ensure_queue_worker_started(app):
    global queue_worker_started

    with progress_lock:
        if queue_worker_started:
            return
        queue_worker_started = True

    t = threading.Thread(target=_queue_worker_loop, args=(app,), daemon=True)
    t.start()


def setup_auto_mkv_hook(app):
    """
    Enregistre un hook post-téléchargement FTP qui enfile automatiquement
    un job MKV si AUTO_MKV_ENABLED est actif.
    Appelé depuis __init__.py au démarrage.
    """
    def _auto_mkv_hook(local_path, filename, media_type):
        with app.app_context():
            if not app.config.get("AUTO_MKV_ENABLED"):
                return

            is_show = str(media_type or "").lower() in {"show", "episode", "series"}
            dst_dir = (
                app.config.get("AUTO_MKV_DST_SHOWS") if is_show
                else app.config.get("AUTO_MKV_DST_MOVIES")
            ) or ""

            if not dst_dir:
                logger.warning("[AUTO_MKV] Destination non configurée pour media_type=%s — job ignoré", media_type)
                return

            if not local_path or not os.path.exists(local_path):
                logger.warning("[AUTO_MKV] Fichier introuvable : %s", local_path)
                return

            mkvmerge_path, mkvpropedit_path = get_mkvtoolnix_binaries(app.config)
            if not mkvmerge_path or not mkvpropedit_path:
                logger.warning("[AUTO_MKV] MKVToolNix introuvable — job ignoré")
                return

            # source_base_dir = dossier parent du fichier téléchargé
            source_base_dir = os.path.dirname(local_path)

            job_id = f"auto_{os.path.splitext(filename)[0]}_{int(time.time())}"

            with progress_lock:
                progress_data[job_id] = {
                    "status": "pending",
                    "current": 0,
                    "total": 1,
                    "percent": 0,
                    "done": False,
                    "cancelled": False,
                    "error": "",
                    "filename": filename,
                    "category": "auto",
                }
                cancel_flags[job_id] = False

            _enqueue_job({
                "job_id": job_id,
                "relative_path": filename,
                "src_path": local_path,
                "source_base_dir": source_base_dir,
                "dst_dir": dst_dir,
                "mkvmerge_path": mkvmerge_path,
                "mkvpropedit_path": mkvpropedit_path,
                "is_series_category": is_show,
                "forced_series_name": "",
            })

            _ensure_queue_worker_started(app)
            logger.info("[AUTO_MKV] Job enfilé : %s → %s", filename, dst_dir)

    register_post_download_hook(_auto_mkv_hook)

def _queue_worker_loop(app):
    logger = logging.getLogger(__name__)
    _idle_cycles = 0

    while True:
        job = _pop_next_pending_job()

        if not job:
            _idle_cycles += 1
            if _idle_cycles >= 150:  # ~60s d'inactivité
                with progress_lock:
                    _purge_old_mkv_jobs()
                _idle_cycles = 0
            time.sleep(0.4)
            continue

        with app.app_context():
            job_id = job["job_id"]
            src_path = job["src_path"]
            source_base_dir = job["source_base_dir"]
            dst_dir = job["dst_dir"]
            mkvmerge_path = job["mkvmerge_path"]
            mkvpropedit_path = job["mkvpropedit_path"]
            is_series_category = job["is_series_category"]

            language_overrides = job.get("language_overrides") or {}
            files_to_process = collect_video_files(src_path)
            total = len(files_to_process)

            if not files_to_process:
                _set_job_state(
                    job_id,
                    status="error",
                    current=0,
                    total=0,
                    percent=0,
                    done=True,
                    cancelled=False,
                    error="Aucun fichier vidéo trouvé",
                    finished_at=time.time(),
                )
                cancel_flags.pop(job_id, None)
                active_processes.pop(job_id, None)
                continue

            _set_job_state(
                job_id,
                status="running",
                current=0,
                total=total,
                percent=0,
                done=False,
                cancelled=False,
                error="",
            )

            errors = []

            try:
                for idx, src_file in enumerate(files_to_process, start=1):
                    with progress_lock:
                        if cancel_flags.get(job_id):
                            progress_data[job_id] = {
                                **progress_data.get(job_id, {}),
                                "status": "cancelled",
                                "current": idx - 1,
                                "total": total,
                                "percent": int(((idx - 1) / max(total, 1)) * 100),
                                "done": True,
                                "cancelled": True,
                                "error": "",
                                "finished_at": time.time(),
                            }
                            cancel_flags.pop(job_id, None)
                            active_processes.pop(job_id, None)
                            break
                    if progress_data.get(job_id, {}).get("status") == "cancelled":
                        break

                    try:
                        forced_series_name = (job.get("forced_series_name") or "").strip()

                        dst_file = build_destination_path(
                            src_file=src_file,
                            src_root=source_base_dir,
                            dst_dir=dst_dir,
                            is_series_category=is_series_category,
                            forced_series_name=forced_series_name,
                        )

                        logger.info(f"[MKV] queue remux start | job={job_id} | src={src_file} | dst={dst_file}")

                        def _make_progress_cb(_job_id, _idx, _total):
                            def _cb(sub_pct):
                                with progress_lock:
                                    if _job_id in progress_data:
                                        pct = int(((_idx - 1 + sub_pct / 100) / max(_total, 1)) * 100)
                                        progress_data[_job_id]["percent"] = pct
                            return _cb

                        remux_file(
                            src_file=src_file,
                            dst_file=dst_file,
                            mkvmerge_path=mkvmerge_path,
                            mkvpropedit_path=mkvpropedit_path,
                            job_id=job_id,
                            active_processes=active_processes,
                            cancel_flags=cancel_flags,
                            progress_lock=progress_lock,
                            progress_callback=_make_progress_cb(job_id, idx, total),
                            language_overrides=language_overrides,
                        )

                        percent = int((idx / max(total, 1)) * 100)

                        with progress_lock:
                            if cancel_flags.get(job_id):
                                progress_data[job_id] = {
                                    **progress_data.get(job_id, {}),
                                    "status": "cancelled",
                                    "current": idx,
                                    "total": total,
                                    "percent": percent,
                                    "done": True,
                                    "cancelled": True,
                                    "error": "",
                                    "finished_at": time.time(),
                                }
                                cancel_flags.pop(job_id, None)
                                active_processes.pop(job_id, None)
                                break

                            progress_data[job_id] = {
                                **progress_data.get(job_id, {}),
                                "status": "running",
                                "current": idx,
                                "total": total,
                                "percent": percent,
                                "done": False,
                                "cancelled": False,
                                "error": "",
                            }

                    except MKVCancelledError:
                        with progress_lock:
                            progress_data[job_id] = {
                                **progress_data.get(job_id, {}),
                                "status": "cancelled",
                                "current": idx - 1,
                                "total": total,
                                "percent": int(((idx - 1) / max(total, 1)) * 100),
                                "done": True,
                                "cancelled": True,
                                "error": "",
                                "finished_at": time.time(),
                            }
                            cancel_flags.pop(job_id, None)
                            active_processes.pop(job_id, None)
                        break

                    except Exception as e:
                        logger.exception(f"[MKV] queued file failed | job={job_id} | src={src_file}")
                        errors.append(f"{os.path.basename(src_file)}: {e}")

                        with progress_lock:
                            progress_data[job_id] = {
                                **progress_data.get(job_id, {}),
                                "status": "running",
                                "current": idx,
                                "total": total,
                                "percent": int((idx / max(total, 1)) * 100),
                                "done": False,
                                "cancelled": False,
                                "error": "; ".join(errors),
                            }

                else:
                    with progress_lock:
                        progress_data[job_id] = {
                            **progress_data.get(job_id, {}),
                            "status": "done",
                            "current": total,
                            "total": total,
                            "percent": 100,
                            "done": True,
                            "cancelled": False,
                            "error": "; ".join(errors) if errors else "",
                            "finished_at": time.time(),
                        }
                        cancel_flags.pop(job_id, None)
                        active_processes.pop(job_id, None)

            except Exception:
                logger.exception(f"[MKV] queue worker crashed on job={job_id}")
                with progress_lock:
                    progress_data[job_id] = {
                        **progress_data.get(job_id, {}),
                        "status": "error",
                        "current": 0,
                        "total": total,
                        "percent": 0,
                        "done": True,
                        "cancelled": False,
                        "error": "Erreur worker",
                        "finished_at": time.time(),
                    }
                    cancel_flags.pop(job_id, None)
                    active_processes.pop(job_id, None)    

@mkv_bp.route("/mkvtoolnix")
def mkvtoolnix_dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("auth.login"))

    # 0) Présence MKVToolNix
    mkvmerge_path, mkvpropedit_path = get_mkvtoolnix_binaries(current_app.config)
    mkvtoolnix_configured = bool(mkvmerge_path and mkvpropedit_path)

    destinations = current_app.config.get("DESTINATIONS_MKV", {}) or {}
    source_dirs = current_app.config.get("MKV_SOURCE_DIRS", []) or []

    if not mkvtoolnix_configured:
        source_space = [get_disk_usage_info(src) for src in source_dirs]
        destination_space = {name: get_disk_usage_info(path) for name, path in destinations.items()}

        return render_template(
            "mkvtoolnix.html",
            mkvtoolnix_configured=False,
            files=[],
            folders=[],
            destinations=destinations,
            film_categories=[],
            serie_categories=[],
            movie_count=0,
            series_count=0,
            source_space=source_space,
            destination_space=destination_space,
            mkv_language_list=load_mkv_languages(),
        )

    # 1) Paramètres UI/perf
    deep = request.args.get("deep") == "1"
    lang_limit = 0 if deep else int(current_app.config.get("MKV_LANG_PROBES_LIMIT", 60))

    plex_token = session.get("plex_token")
    plex_server = _get_local_plex_server(plex_token)

    # 2) Classement des catégories via Plex
    film_categories = []
    serie_categories = []

    for category_name in destinations.keys():
        try:
            # Réutilise la connexion plex_server déjà établie — évite N connexions MyPlexAccount
            if _is_series_with_server(plex_server, category_name):
                serie_categories.append(category_name)
            else:
                film_categories.append(category_name)
        except Exception as e:
            current_app.logger.warning(
                f"[MKV UI] _is_series_with_server a échoué pour '{category_name}': {e}"
            )
            if _is_series_category_name(category_name):
                serie_categories.append(category_name)
            else:
                film_categories.append(category_name)

    film_categories = sorted(film_categories)
    serie_categories = sorted(serie_categories)

    # 3) Scan des sources (logique inspirée de ta version Windows)
    video_exts = (".mkv", ".avi", ".mov", ".mp4", ".mpeg")
    files_accum = []
    folders_accum = {}

    for base_dir in source_dirs:
        if not os.path.isdir(base_dir):
            current_app.logger.warning(f"[MKV UI] Dossier source introuvable : {base_dir}")
            continue

        for root, dirs, filenames in os.walk(base_dir):
            dirs[:] = [d for d in dirs if d not in IGNORED_DIRS]

            for filename in filenames:
                if not filename.lower().endswith(video_exts):
                    continue

                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, base_dir)
                parts = [p for p in rel_path.split(os.sep) if p]

                # Cas spécial Plex Sync (repris de ta version Windows)
                if "Plex Media Server" in base_dir:
                    if "13" in parts:
                        try:
                            idx = parts.index("13")
                            if len(parts) > idx + 1:
                                folder_key = os.path.join(*parts[:idx + 2])
                                entry = folders_accum.get(folder_key)
                                if not entry:
                                    folders_accum[folder_key] = {
                                        "count": 1,
                                        "first_mkv": full_path
                                    }
                                else:
                                    entry["count"] += 1
                                continue
                        except Exception:
                            pass

                    files_accum.append({"abs": full_path, "rel": rel_path})
                    continue

                # Logique officielle inbox :
                # - profondeur 2 => film (Source/Fichier.mkv)
                # - profondeur >= 3 => série (Source/Série/Saison/Épisode.mkv)
                if len(parts) == 2:
                    files_accum.append({"abs": full_path, "rel": rel_path})
                elif len(parts) >= 3:
                    folder_key = os.path.join(parts[0], parts[1])
                    entry = folders_accum.get(folder_key)
                    if not entry:
                        folders_accum[folder_key] = {
                            "count": 1,
                            "first_mkv": full_path
                        }
                    else:
                        entry["count"] += 1
                else:
                    # profondeur 1 (rare) => film
                    files_accum.append({"abs": full_path, "rel": rel_path})

    files_accum.sort(key=lambda x: x["rel"].lower())
    folders_items = [
        {"folder": k, "count": v["count"], "first_mkv": v["first_mkv"]}
        for k, v in sorted(folders_accum.items(), key=lambda it: it[0].lower())
    ]

    # 4) Préparer pour le template
    files = []
    for i, f in enumerate(files_accum, 1):
        if lang_limit == 0 or i <= lang_limit:
            langs = extract_languages(f["abs"], mkvmerge_path)
        else:
            langs = []

        status = get_mkv_status(langs) if langs else "Non sondé"
        files.append({
            "path": f["rel"],
            "b64": base64.b64encode(f["rel"].encode("utf-8")).decode("utf-8"),
            "languages": langs,
            "status": status
        })

    encoded_folders = []
    for j, item in enumerate(folders_items, 1):
        first_abs = item["first_mkv"]

        if lang_limit == 0 or j <= lang_limit:
            langs = extract_languages(first_abs, mkvmerge_path)
        else:
            langs = []

        status = get_mkv_status(langs) if langs else "Non sondé"

        plex_title, plex_year = _resolve_plex_show_name(plex_server, item["folder"])

        encoded_folders.append({
            "folder": item["folder"],
            "count": item["count"],
            "b64": base64.b64encode(item["folder"].encode("utf-8")).decode("utf-8"),
            "languages": langs,
            "status": status,
            "plex_title": plex_title or "",
            "plex_year": plex_year or "",
        })

    # 5) Espace disque
    source_space = [get_disk_usage_info(src) for src in source_dirs]
    destination_space = {name: get_disk_usage_info(path) for name, path in destinations.items()}

    current_app.logger.info(f"[MKV] source_dirs = {source_dirs}")
    current_app.logger.info(f"[MKV] destinations = {destinations}")
    current_app.logger.warning(f"[MKV] source_space = {source_space}")
    current_app.logger.warning(f"[MKV] destination_space = {destination_space}")

    return render_template(
        "mkvtoolnix.html",
        mkvtoolnix_configured=True,
        files=files,
        folders=encoded_folders,
        destinations=destinations,
        film_categories=film_categories,
        serie_categories=serie_categories,
        movie_count=len(files),
        series_count=len(encoded_folders),
        source_space=source_space,
        destination_space=destination_space,
        mkv_language_list=load_mkv_languages(),
    )

@mkv_bp.route("/mkv_languages")
def mkv_languages():
    if not session.get("logged_in"):
        return jsonify({"ok": False}), 401
    return jsonify({"ok": True, "languages": load_mkv_languages()})


@mkv_bp.route("/mkv_languages", methods=["POST"])
def mkv_languages_add():
    if not session.get("logged_in"):
        return jsonify({"ok": False}), 401
    data = request.get_json(silent=True) or {}
    code = (data.get("code") or "").strip()
    if not code:
        return jsonify({"ok": False, "message": "Code vide"}), 400
    from ..services.mkvtoolnix_service import register_mkv_languages
    register_mkv_languages([code])
    return jsonify({"ok": True, "languages": load_mkv_languages()})


@mkv_bp.route("/process_mkv", methods=["POST"])
def process_mkv():
    try:
        if not session.get("logged_in"):
            return jsonify({"status": "error", "message": "Non connecté"}), 401

        data = request.get_json(silent=True) or {}
        relative_path = data.get("filename", "")
        category = data.get("category", "")
        job_id = data.get("job_id", "")
        plex_title = (data.get("plex_title") or "").strip()
        plex_year = data.get("plex_year")
        language_overrides = data.get("language_overrides") or {}

        current_app.logger.info(
            f"[MKV] process_mkv start | filename={relative_path} | category={category} | job_id={job_id} | plex_title={plex_title} | plex_year={plex_year}"
        )

        if not relative_path or not category or not job_id:
            return jsonify({"status": "error", "message": "Paramètres manquants"}), 400

        destinations = current_app.config.get("DESTINATIONS_MKV", {}) or {}
        dst_dir = destinations.get(category)
        if not dst_dir:
            return jsonify({"status": "error", "message": "Destination invalide"}), 400

        source_dirs = current_app.config.get("MKV_SOURCE_DIRS", []) or []
        src_path, source_base_dir = find_source_path(relative_path, source_dirs)

        current_app.logger.info(
            f"[MKV] resolved source | src_path={src_path} | source_base_dir={source_base_dir} | dst_dir={dst_dir}"
        )

        if not src_path:
            return jsonify({"status": "error", "message": f"Source introuvable : {relative_path}"}), 404

        mkvmerge_path, mkvpropedit_path = get_mkvtoolnix_binaries(current_app.config)
        current_app.logger.info(
            f"[MKV] binaries | mkvmerge={mkvmerge_path} | mkvpropedit={mkvpropedit_path}"
        )

        if not (mkvmerge_path and mkvpropedit_path):
            return jsonify({"status": "error", "message": "MKVToolNix introuvable"}), 500

        files_to_process = collect_video_files(src_path)
        current_app.logger.info(f"[MKV] files_to_process={len(files_to_process)}")

        if not files_to_process:
            return jsonify({"status": "error", "message": "Aucun fichier vidéo trouvé"}), 400

        plex_token = session.get("plex_token")
        is_series_category = resolve_category_is_series(plex_token, category)

        forced_series_name = ""
        if is_series_category and plex_title:
            forced_series_name = plex_title
            if plex_year:
                forced_series_name = f"{plex_title} ({plex_year})"
        current_app.logger.info(
            f"[MKV] forced_series_name={forced_series_name} | plex_title={plex_title} | plex_year={plex_year}"
        )        

        with progress_lock:
            progress_data[job_id] = {
                "status": "pending",
                "current": 0,
                "total": len(files_to_process),
                "percent": 0,
                "done": False,
                "cancelled": False,
                "error": "",
                "filename": relative_path,
                "category": category,
            }
            cancel_flags[job_id] = False

        _enqueue_job({
            "job_id": job_id,
            "relative_path": relative_path,
            "src_path": src_path,
            "source_base_dir": source_base_dir,
            "dst_dir": dst_dir,
            "mkvmerge_path": mkvmerge_path,
            "mkvpropedit_path": mkvpropedit_path,
            "is_series_category": is_series_category,
            "forced_series_name": forced_series_name,
            "language_overrides": language_overrides,
        })

        _ensure_queue_worker_started(current_app._get_current_object())

        return jsonify({
            "status": "queued",
            "message": "Job ajouté à la file",
            "job_id": job_id,
        })
    
    except Exception as e:
        current_app.logger.exception("[MKV] process_mkv crashed before queue")
        return jsonify({"status": "error", "message": f"Erreur process_mkv: {e}"}), 500
        
@mkv_bp.route("/cancel_mkv", methods=["POST"])
def cancel_mkv():
    if not session.get("logged_in"):
        return jsonify({"status": "error", "message": "Non connecté"}), 401

    data = request.get_json(silent=True) or {}
    job_id = data.get("job_id", "")

    if not job_id:
        return jsonify({"status": "error", "message": "job_id manquant"}), 400

    with progress_lock:
        state = progress_data.get(job_id, {})
        status = state.get("status")
        cancel_flags[job_id] = True

        if status == "pending":
            progress_data[job_id] = {
                **state,
                "status": "cancelled",
                "done": True,
                "cancelled": True,
                "error": "",
            }

        proc = active_processes.get(job_id)

    if proc:
        try:
            proc.terminate()
        except Exception:
            pass

    return jsonify({"status": "ok", "message": "Annulation demandée", "job_id": job_id})

@mkv_bp.route("/mkv_status")
def mkv_status():
    """Endpoint léger pour le badge de progression dans la barre de navigation."""
    if not session.get("logged_in"):
        return jsonify({"status": "idle"})

    with progress_lock:
        for job_id, state in progress_data.items():
            s = state.get("status", "")
            if s in ("running", "pending"):
                return jsonify({
                    "status": s,
                    "job_id": job_id,
                    "percent": state.get("percent", 0),
                    "current": state.get("current", 0),
                    "total": state.get("total", 0),
                    "filename": state.get("filename", ""),
                })

    return jsonify({"status": "idle"})


@mkv_bp.route("/delete_mkv", methods=["POST"])
def delete_mkv():
    if not session.get("logged_in"):
        return jsonify({"status": "error", "message": "Non connecté"}), 401

    data = request.get_json(silent=True) or {}
    relative_path = data.get("filename", "")

    if not relative_path:
        return jsonify({"status": "error", "message": "Paramètre manquant"}), 400

    source_dirs = current_app.config.get("MKV_SOURCE_DIRS", []) or []
    src_path, _ = find_source_path(relative_path, source_dirs)

    if not src_path:
        return jsonify({"status": "error", "message": "Chemin introuvable"}), 404

    # Sécurité : vérifier que le chemin résolu est bien sous un répertoire source
    real_path = os.path.realpath(src_path)
    if not any(real_path.startswith(os.path.realpath(d) + os.sep) or real_path == os.path.realpath(d)
               for d in source_dirs):
        current_app.logger.warning(f"[MKV] delete refusé (hors source) : {real_path}")
        return jsonify({"status": "error", "message": "Chemin non autorisé"}), 403

    try:
        if os.path.isdir(src_path):
            shutil.rmtree(src_path)
            current_app.logger.info(f"[MKV] dossier supprimé : {src_path}")
        elif os.path.isfile(src_path):
            os.remove(src_path)
            current_app.logger.info(f"[MKV] fichier supprimé : {src_path}")
        else:
            return jsonify({"status": "error", "message": "Introuvable"}), 404

        return jsonify({"status": "ok"})

    except Exception as e:
        current_app.logger.error(f"[MKV] suppression échouée ({src_path}) : {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@mkv_bp.route("/progress_stream/<job_id>")
def progress_stream(job_id):
    def event_stream():
        max_iterations = int(12 * 3600 / 0.4)  # 12h max
        for _ in range(max_iterations):
            with progress_lock:
                data = progress_data.get(job_id)

            if data:
                yield f"data: {json.dumps(data)}\n\n"
                if data.get("done"):
                    break
            else:
                yield 'data: {"percent": 0, "done": false, "status": "unknown"}\n\n'

            time.sleep(0.4)

    return Response(
        event_stream(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )