import os
import re
import uuid
import time
import logging
import threading
from collections import deque
from flask import current_app
from app.services.ftp_service import get_ftp_client, ftp_download_file
from app.services.mkvtoolnix_service import parse_episode_info
from typing import Optional, Tuple

ftp_download_jobs: dict = {}
ftp_download_jobs_lock = threading.Lock()
ftp_download_queue: deque = deque(maxlen=500)
ftp_cancel_flags: dict = {}
ftp_worker_started = False

# Jobs terminés (done / error / cancelled) conservés max N secondes avant purge
_JOB_TTL_SECONDS = 3600  # 1 heure

# Compteur journalier pour le mode auto
_daily_counter = {"date": None, "count": 0}
_daily_counter_lock = threading.Lock()

# Hook post-téléchargement (enregistré par mkv_routes au démarrage)
_post_download_hook = None


def register_post_download_hook(fn):
    """
    Enregistre une fonction appelée après chaque téléchargement réussi.
    Signature : fn(local_path, filename, media_type)
    """
    global _post_download_hook
    _post_download_hook = fn


def _should_process_now(app) -> bool:
    """
    Vérifie si les conditions de planification automatique sont réunies.
    Si AUTO_DOWNLOAD_ENABLED est False, retourne toujours True (pas de restriction).
    """
    from datetime import datetime
    if not bool(app.config.get("AUTO_DOWNLOAD_ENABLED", False)):
        return True

    now = datetime.now()

    # Jours du mois à sauter
    skip_days = app.config.get("FTP_AUTO_SKIP_DAYS") or []
    if now.day in skip_days:
        return False

    # Fenêtre horaire
    start_str = str(app.config.get("AUTO_DOWNLOAD_START") or "00:00")
    end_str = str(app.config.get("AUTO_DOWNLOAD_END") or "06:00")
    try:
        sh, sm = [int(x) for x in start_str.split(":")]
        eh, em = [int(x) for x in end_str.split(":")]
    except Exception:
        sh, sm, eh, em = 0, 0, 6, 0

    start_min = sh * 60 + sm
    end_min = eh * 60 + em
    current_min = now.hour * 60 + now.minute

    if start_min <= end_min:
        in_window = start_min <= current_min < end_min
    else:  # fenêtre qui franchit minuit (ex: 22:00 → 06:00)
        in_window = current_min >= start_min or current_min < end_min

    if not in_window:
        return False

    # Limite journalière
    max_per_day = int(app.config.get("FTP_AUTO_MAX_PER_DAY") or 0)
    if max_per_day > 0:
        today = now.strftime("%Y-%m-%d")
        with _daily_counter_lock:
            if _daily_counter["date"] != today:
                _daily_counter["date"] = today
                _daily_counter["count"] = 0
            if _daily_counter["count"] >= max_per_day:
                return False

    return True


def _increment_daily_counter():
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    with _daily_counter_lock:
        if _daily_counter["date"] != today:
            _daily_counter["date"] = today
            _daily_counter["count"] = 0
        _daily_counter["count"] += 1


def _purge_old_jobs():
    """Supprime les jobs terminés plus vieux que _JOB_TTL_SECONDS.
    Doit être appelé sous ftp_download_jobs_lock."""
    now = time.time()
    terminal_statuses = {"done", "error", "cancelled"}
    to_delete = [
        job_id
        for job_id, entry in ftp_download_jobs.items()
        if entry.get("job", {}).get("status") in terminal_statuses
        and now - entry.get("finished_at", now) > _JOB_TTL_SECONDS
    ]
    for job_id in to_delete:
        del ftp_download_jobs[job_id]
    if to_delete:
        logging.getLogger(__name__).debug(
            "[FTP] Purged %d old jobs from memory", len(to_delete)
        )

def _clean_source_media_guess(name: str) -> Tuple[str, Optional[int]]:
    """
    Transforme un nom source/release en guess propre:
    'La.Candidate.2023.S01.FRENCH.1080p.WEB.H264-TFA'
    -> ('La Candidate', 2023)
    """
    if not name:
        return "", None

    raw = os.path.splitext(os.path.basename(name))[0]
    txt = raw.replace("_", " ").replace(".", " ").strip()

    year = None
    m_year = re.search(r"\b(19\d{2}|20\d{2})\b", txt)
    if m_year:
        try:
            year = int(m_year.group(1))
        except Exception:
            year = None

    stop_match = re.search(
        r"(?i)\b("
        r"S\d{1,2}E\d{1,2}|S\d{1,2}|Season\s*\d+|Saison\s*\d+|"
        r"2160p|1080p|720p|480p|WEB[- ]?DL|WEBRip|BluRay|BRRip|DVDRip|HDRip|Remux|"
        r"x264|x265|h264|h265|HEVC|AAC|AC3|DTS|DDP5\.1|Atmos|"
        r"FRENCH|VFF|VFQ|MULTI|TRUEFRENCH|SUBFRENCH|COMPLETE|PROPER|REPACK"
        r")\b",
        txt,
    )
    if stop_match:
        txt = txt[:stop_match.start()].strip()

    if year is not None:
        txt = re.sub(rf"\b{year}\b", " ", txt)

    txt = re.sub(r"\s+", " ", txt).strip(" -._")

    return txt, year

def normalize_episode_filename(filename):
    name, ext = os.path.splitext(filename)

    match = re.search(r"(\d{1,2})x(\d{1,2})", name, re.IGNORECASE)
    if match:
        season = int(match.group(1))
        episode = int(match.group(2))
        new_pattern = f"S{season:02d}E{episode:02d}"
        name = re.sub(r"(\d{1,2})x(\d{1,2})", new_pattern, name, flags=re.IGNORECASE)

    return name + ext


def _sanitize_filename(name):
    if not name:
        return "Unknown"
    return re.sub(r'[<>:"/\\|?*]+', "", str(name)).strip().rstrip(".")


def _guess_series_folder_from_filename(filename):
    name = os.path.splitext(os.path.basename(filename))[0]

    parts = re.split(
        r"\s*-\s*(?:S\d{2}E\d{2}|\d{1,2}x\d{1,2})\b",
        name,
        maxsplit=1,
        flags=re.IGNORECASE,
    )
    if parts and parts[0].strip():
        return parts[0].strip()

    return name.strip()


def _build_local_path(filename, media_type="movie", ftp_id=None):
    if not filename:
        raise ValueError("filename manquant")

    media_type_norm = str(media_type or "movie").lower()
    is_show = media_type_norm in {"show", "episode", "series"}

    if is_show:
        filename = normalize_episode_filename(filename)

    srv = None
    if ftp_id:
        servers = current_app.config.get("FTP_SERVERS") or []
        srv = next((s for s in servers if s.get("id") == ftp_id), None)

    if is_show:
        base_dir = (
            (srv.get("download_dir_shows") if srv else None)
            or current_app.config.get("FTP_DOWNLOAD_DIR_SHOWS")
            or ""
        ).strip()
    else:
        base_dir = (
            (srv.get("download_dir_movies") if srv else None)
            or current_app.config.get("FTP_DOWNLOAD_DIR_MOVIES")
            or ""
        ).strip()

    if not base_dir:
        raise ValueError("Dossier de téléchargement FTP non configuré.")

    if is_show:
        # 🔍 Extraire infos épisode
        season, episode, _ = parse_episode_info(filename)

        # 🧠 Nettoyer le nom de série
        series_name_raw = _guess_series_folder_from_filename(filename)
        series_name_clean, year = _clean_source_media_guess(series_name_raw)

        if not series_name_clean:
            series_name_clean = series_name_raw or "Série inconnue"

        # 🎬 Ajouter l'année si trouvée
        if year:
            series_folder = f"{series_name_clean} ({year})"
        else:
            series_folder = series_name_clean

        series_folder = _sanitize_filename(series_folder)

        # 📂 Saison
        if season is not None:
            season_folder = f"Season {season:02d}"
            local_dir = os.path.join(base_dir, series_folder, season_folder)
        else:
            local_dir = os.path.join(base_dir, series_folder)

    else:
        local_dir = base_dir

    os.makedirs(local_dir, exist_ok=True)
    try:
        os.chmod(local_dir, 0o777)
    except Exception:
        pass
    return os.path.join(local_dir, filename), filename


def _get_ftp_config(ftp_id=None):
    if ftp_id:
        servers = current_app.config.get("FTP_SERVERS") or []
        srv = next((s for s in servers if s.get("id") == ftp_id), None)
        if srv:
            return {
                "host": (srv.get("host") or "").strip(),
                "port": int(srv.get("port", 21)),
                "username": (srv.get("user") or "").strip(),
                "password": srv.get("pass") or "",
                "use_tls": bool(srv.get("tls", False)),
                "passive": bool(srv.get("passive", True)),
            }
    return {
        "host": (current_app.config.get("FTP_HOST") or "").strip(),
        "port": int(current_app.config.get("FTP_PORT", 21)),
        "username": (current_app.config.get("FTP_USER") or "").strip(),
        "password": current_app.config.get("FTP_PASS") or "",
        "use_tls": bool(current_app.config.get("FTP_TLS", False)),
        "passive": bool(current_app.config.get("FTP_PASSIVE", True)),
    }


def create_ftp_download_job(remote_path, filename=None, media_type="movie", media_key=None, ftp_id=None):
    effective_media_key = media_key or remote_path

    if not remote_path:
        raise ValueError("remote_path manquant")

    if not filename:
        filename = os.path.basename(remote_path.rstrip("/"))

    local_path, final_filename = _build_local_path(filename, media_type, ftp_id=ftp_id)
    ftp_cfg = _get_ftp_config(ftp_id=ftp_id)
    job_id = uuid.uuid4().hex

    with ftp_download_jobs_lock:
        _purge_old_jobs()

        ftp_download_jobs[job_id] = {
            "success": True,
            "finished_at": None,
            "job": {
                "job_id": job_id,
                "media_key": effective_media_key,
                "status": "queued",
                "remote_path": remote_path,
                "local_path": local_path,
                "filename": final_filename,
                "media_type": media_type,
                "ftp_id": ftp_id,
                "percent": 0,
                "error": None,
                **ftp_cfg,
            }
        }

        ftp_download_queue.append(job_id)
        queue_position = len(ftp_download_queue)

    logging.getLogger(__name__).debug(f"[FTP QUEUE] appended job_id={job_id} queue={ftp_download_queue}")
    return job_id, queue_position

def cancel_ftp_job(job_id):
    if not job_id:
        return {"success": False, "error": "job_id manquant"}

    with ftp_download_jobs_lock:
        job_entry = ftp_download_jobs.get(job_id)
        if not job_entry:
            return {"success": False, "error": "Job introuvable"}

        status = job_entry["job"].get("status")

        if status == "queued":
            if job_id in ftp_download_queue:
                ftp_download_queue.remove(job_id)
                job_entry["job"]["status"] = "cancelled"
                job_entry["job"]["percent"] = 0
                job_entry["job"]["error"] = None
                job_entry["finished_at"] = time.time()
                return {"success": True, "status": "cancelled"}

            # Le job n'est plus dans la file: il est probablement déjà pris par le worker
            job_entry["job"]["status"] = "downloading"
            return {
                "success": False,
                "error": "Le téléchargement est déjà en cours"
            }

        if status == "downloading":
            return {
                "success": False,
                "error": "Impossible de retirer un téléchargement en cours"
            }

        return {
            "success": False,
            "error": f"Impossible d'annuler un job avec le statut '{status}'",
        }

def get_download_status(job_id):
    if not job_id:
        return {"success": False, "error": "job_id manquant"}

    with ftp_download_jobs_lock:
        job_entry = ftp_download_jobs.get(job_id)
        if not job_entry:
            return {"success": False, "error": "job introuvable"}

        job = job_entry.get("job", {})
        queue_position = None

        if job.get("status") == "queued" and job_id in ftp_download_queue:
            queue_position = ftp_download_queue.index(job_id) + 1

        return {
            "success": True,
            "job_id": job.get("job_id"),
            "status": job.get("status", "idle"),
            "progress": job.get("percent", 0),
            "filename": job.get("filename"),
            "error": job.get("error"),
            "queue_position": queue_position,
        }

def get_download_status_by_media_key(media_key):
    if not media_key:
        return {"success": False, "error": "media_key manquant"}

    with ftp_download_jobs_lock:
        for job_entry in reversed(list(ftp_download_jobs.values())):
            job = job_entry.get("job", {})
            if job.get("media_key") == media_key:
                job_id = job.get("job_id")
                queue_position = None

                if job.get("status") == "queued" and job_id in ftp_download_queue:
                    queue_position = ftp_download_queue.index(job_id) + 1

                return {
                    "success": True,
                    "job_id": job_id,
                    "status": job.get("status", "idle"),
                    "progress": job.get("percent", 0),
                    "filename": job.get("filename"),
                    "error": job.get("error"),
                    "queue_position": queue_position,
                }

    return {"success": True, "status": "idle", "progress": 0}

def _run_ftp_download_job(job_id):
    ftp = None
    logging.getLogger(__name__).debug(f"[FTP RUN] start job_id={job_id}")

    with ftp_download_jobs_lock:
        job_entry = ftp_download_jobs.get(job_id)
        if not job_entry:
            logging.getLogger(__name__).debug(f"[FTP RUN] job_id={job_id} absent")
            return

        job = job_entry["job"]

        if job.get("status") == "cancelled":
            logging.getLogger(__name__).debug(f"[FTP RUN] job_id={job_id} déjà cancelled")
            return

        remote_path = job["remote_path"]
        local_path = job["local_path"]
        host = job["host"]
        port = job["port"]
        username = job["username"]
        password = job["password"]
        use_tls = job["use_tls"]
        passive = job["passive"]

        job["status"] = "downloading"
        job["percent"] = 0

    logging.getLogger(__name__).debug(f"[FTP RUN] connecting host={host}:{port} remote={remote_path} local={local_path}")

    try:
        ftp = get_ftp_client(
            host=host,
            port=port,
            username=username,
            password=password,
            use_tls=use_tls,
            passive=passive,
        )

        logging.getLogger(__name__).debug(f"[FTP RUN] connected job_id={job_id}")

        def progress_callback(downloaded, total, percent):
            with ftp_download_jobs_lock:
                job_entry = ftp_download_jobs.get(job_id)
                if not job_entry:
                    return

                job_entry["job"]["status"] = "downloading"
                job_entry["job"]["percent"] = percent

        ftp_download_file(
            ftp=ftp,
            remote_path=remote_path,
            local_path=local_path,
            progress_callback=progress_callback,
        )

        logging.getLogger(__name__).debug(f"[FTP RUN] download complete job_id={job_id}")

        with ftp_download_jobs_lock:
            job_entry = ftp_download_jobs.get(job_id)
            if job_entry:
                job_entry["job"]["status"] = "done"
                job_entry["job"]["percent"] = 100
                job_entry["job"]["error"] = None
                job_entry["finished_at"] = time.time()

        # Hook post-téléchargement (ex: auto-remux MKV)
        if _post_download_hook:
            try:
                with ftp_download_jobs_lock:
                    job_entry = ftp_download_jobs.get(job_id, {})
                    job = job_entry.get("job", {})
                _post_download_hook(
                    local_path=job.get("local_path", ""),
                    filename=job.get("filename", ""),
                    media_type=job.get("media_type", "movie"),
                )
            except Exception as _hook_err:
                logging.getLogger(__name__).warning("[FTP] post_download_hook error: %s", _hook_err)

    except Exception as e:
        logging.getLogger(__name__).error(f"[FTP RUN ERROR] job_id={job_id} error={e}")

        with ftp_download_jobs_lock:
            job_entry = ftp_download_jobs.get(job_id)
            if job_entry:
                job_entry["job"]["status"] = "error"
                job_entry["job"]["error"] = str(e)
                job_entry["finished_at"] = time.time()

    finally:
        if ftp:
            try:
                ftp.quit()
            except Exception:
                pass



def _ftp_queue_worker(app):
    logger = logging.getLogger(__name__)
    _idle_cycles = 0
    while True:
        # Vérifier les conditions de planification avant de dépiler
        if not _should_process_now(app):
            time.sleep(30)  # Réévaluer toutes les 30s
            continue

        job_id = None

        with ftp_download_jobs_lock:
            if ftp_download_queue:
                job_id = ftp_download_queue.popleft()  # O(1) vs list.pop(0) O(n)
            else:
                _idle_cycles += 1
                if _idle_cycles >= 120:  # ~60s d'inactivité
                    _purge_old_jobs()
                    _idle_cycles = 0

        if not job_id:
            time.sleep(0.5)
            continue

        try:
            with ftp_download_jobs_lock:
                job_entry = ftp_download_jobs.get(job_id)
                if not job_entry:
                    continue
                if job_entry["job"].get("status") == "cancelled":
                    continue

            _run_ftp_download_job(job_id)

            # Incrémenter le compteur journalier si le mode auto est actif
            if bool(app.config.get("AUTO_DOWNLOAD_ENABLED", False)):
                _increment_daily_counter()

        except Exception as e:
            logger.error(f"[FTP WORKER ERROR] job_id={job_id} error={e}")

def start_ftp_queue_worker(app):
    global ftp_worker_started

    with ftp_download_jobs_lock:
        if ftp_worker_started:
            return

        thread = threading.Thread(target=_ftp_queue_worker, args=(app,), daemon=True)
        thread.start()
        ftp_worker_started = True

def move_ftp_job_up(job_id):
    with ftp_download_jobs_lock:
        if job_id not in ftp_download_queue:
            return {"success": False, "error": "Job non trouvé dans la file"}

        idx = ftp_download_queue.index(job_id)
        if idx == 0:
            return {"success": True, "status": "already_first"}

        ftp_download_queue[idx], ftp_download_queue[idx - 1] = (
            ftp_download_queue[idx - 1],
            ftp_download_queue[idx],
        )

        new_index = ftp_download_queue.index(job_id)

        return {
            "success": True,
            "status": "moved_up",
            "queue_position": new_index + 1
        }

def move_ftp_job_down(job_id):
    with ftp_download_jobs_lock:
        if job_id not in ftp_download_queue:
            return {"success": False, "error": "Job non trouvé dans la file"}

        idx = ftp_download_queue.index(job_id)
        if idx >= len(ftp_download_queue) - 1:
            return {"success": True, "status": "already_last"}

        ftp_download_queue[idx], ftp_download_queue[idx + 1] = (
            ftp_download_queue[idx + 1],
            ftp_download_queue[idx],
        )

        return {"success": True, "status": "moved_down", "queue_position": idx + 2}     

def promote_ftp_job(job_id):
    with ftp_download_jobs_lock:
        if job_id not in ftp_download_queue:
            return {"success": False, "error": "Job non trouvé dans la file"}

        ftp_download_queue.remove(job_id)
        ftp_download_queue.insert(0, job_id)

        return {"success": True, "status": "promoted", "queue_position": 1}           