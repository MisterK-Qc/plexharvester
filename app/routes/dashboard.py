from flask import Blueprint, render_template, session, redirect, url_for, request, current_app, jsonify
from plexapi.myplex import MyPlexAccount
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import os
import json
import hashlib

from ..services.cache_service import load_cache, save_cache, is_cache_valid
from ..services.plex_service import connect_to_server, serialize_section, build_indexes, find_match
from ..services.compare_service import compare_movie, compare_show, compare_artist_albums
from ..services.ignore_service import is_ignored
from ..services.ftp_index_service import (
    find_ftp_matches_for_movie,
    find_ftp_match_for_episode,
    ftp_index_status,
    ftp_index_lock,
    build_ftp_index,
    ensure_ftp_index,
    load_ftp_index,
)

dashboard_bp = Blueprint("dashboard", __name__)

# ── File-based background computation tracking ─────────────────────────────
# Status is written to /tmp so it survives Gunicorn worker restarts.
_STATUS_DIR = "/tmp/plexharvester_dash"


def _status_file(cache_key):
    h = hashlib.md5(cache_key.encode()).hexdigest()[:16]
    return os.path.join(_STATUS_DIR, f"status_{h}.json")


def _set_bg_status(cache_key, status, percent=0, message=""):
    os.makedirs(_STATUS_DIR, exist_ok=True)
    data = {"status": status, "percent": percent, "message": message, "pid": os.getpid()}
    try:
        tmp = _status_file(cache_key) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, _status_file(cache_key))
    except Exception:
        pass


def _get_bg_status(cache_key):
    try:
        with open(_status_file(cache_key)) as f:
            data = json.load(f)
        # Si status=running mais le PID n'existe plus → computation morte
        if data.get("status") == "running":
            pid = data.get("pid")
            if pid:
                try:
                    os.kill(pid, 0)  # signal 0 = vérification existence seulement
                except (ProcessLookupError, PermissionError):
                    return {"status": "dead", "percent": 0, "message": "Computation interrompue"}
        return data
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _clear_bg_status(cache_key):
    try:
        os.remove(_status_file(cache_key))
    except Exception:
        pass


def _compute_ftp_ids_for_server(app_obj, selected_server_name):
    """Return ftp_ids associated with the given Plex server, or None for no filtering."""
    servers = app_obj.config.get("FTP_SERVERS") or []
    if not servers:
        return None  # backward compat: no multi-FTP config
    matching = [
        s["id"] for s in servers
        if s.get("enabled", True)
        and (not s.get("plex_servers") or selected_server_name in s.get("plex_servers", []))
    ]
    return matching


def get_comparison_settings():
    return {
        "resolution_filter_mode": current_app.config.get("RESOLUTION_FILTER_MODE", "none"),
        "use_bitrate": bool(current_app.config.get("COMPARE_USE_BITRATE", False)),
        "min_bitrate_diff_pct": float(current_app.config.get("MIN_BITRATE_DIFF_PERCENT", 10)),
        "ignore_transcoded": bool(current_app.config.get("IGNORE_TRANSCODED_IN_BETTER", False)),
        "show_only_labels": [
            str(x).strip().lower()
            for x in current_app.config.get("SHOW_ONLY_LABELS", [])
            if str(x).strip()
        ]
    }


def _compute_dashboard_bg(app_obj, cache_key, plex_token, selected_server_name,
                           selected_libraries, excluded_libs, force_refresh):
    """Runs the full Plex comparison in a background thread and saves to cache."""
    with app_obj.app_context():
        try:
            _set_bg_status(cache_key, "running", 0, "Connexion à Plex...")

            account = MyPlexAccount(token=plex_token)
            resources = account.resources()

            my_server_res = next(
                (s for s in resources if s.provides == "server" and getattr(s, "owned", False)),
                None
            )
            target_server_res = next(
                (res for res in resources if res.name == selected_server_name),
                None
            )

            if not my_server_res or not target_server_res:
                _set_bg_status(cache_key, "error", 0, "Serveur introuvable.")
                return

            my_server = connect_to_server(my_server_res, plex_token, prefer_local=True)
            target_server = connect_to_server(
                target_server_res,
                plex_token,
                prefer_local=bool(getattr(target_server_res, "owned", False))
            )

            if not my_server or not target_server:
                _set_bg_status(cache_key, "error", 0, "Connexion impossible.")
                return

            # ── B1 : Sérialisation des sections des deux serveurs en parallèle ──
            all_sections_to_load = []
            for s in my_server.library.sections():
                if s.title.lower() not in excluded_libs:
                    all_sections_to_load.append(("my", s.title, s))
            for s in target_server.library.sections():
                if s.title.lower() not in excluded_libs:
                    all_sections_to_load.append(("target", s.title, s))

            my_sections = {}
            target_sections = {}

            _b1_total = len(all_sections_to_load)
            _b1_done = [0]

            with ftp_index_lock:
                ftp_index_status.update({
                    "phase": "loading_sections",
                    "running": True,
                    "finished": False,
                    "message": "Chargement des bibliothèques Plex...",
                    "comparison_done": 0,
                    "comparison_total": _b1_total,
                    "comparison_percent": 0,
                    "started_at": time.time(),
                })

            if all_sections_to_load:
                _b1_workers = min(_b1_total, 2)
                with ThreadPoolExecutor(max_workers=_b1_workers) as pool:
                    futures = [
                        (pool.submit(serialize_section, s), origin, title)
                        for origin, title, s in all_sections_to_load
                    ]
                    for fut, origin, title in futures:
                        try:
                            data = fut.result()
                        except Exception as exc:
                            app_obj.logger.warning("[B1] serialize_section '%s': %s", title, exc)
                            data = []
                        _b1_done[0] += 1
                        pct_load = int(_b1_done[0] / _b1_total * 100) if _b1_total else 100
                        with ftp_index_lock:
                            ftp_index_status["message"] = f"Bibliothèque chargée : {title}"
                            ftp_index_status["comparison_done"] = _b1_done[0]
                            ftp_index_status["comparison_percent"] = pct_load
                        _set_bg_status(cache_key, "running", pct_load // 3,
                                       f"Chargement bibliothèques : {_b1_done[0]}/{_b1_total}")
                        if origin == "my":
                            my_sections[title] = data
                        else:
                            target_sections[title] = data

            app_obj.logger.info("[B1] %s sections chargées", _b1_total)

            # ── FTP index ──────────────────────────────────────────────────────
            _set_bg_status(cache_key, "running", 35, "Index FTP...")
            ftp_state = "force_refresh"
            if force_refresh:
                app_obj.logger.info("[DASHBOARD] force_refresh — rebuild FTP")
                with ftp_index_lock:
                    if not ftp_index_status.get("running"):
                        ftp_index_status.update({
                            "running": True, "finished": False, "phase": "starting",
                            "current_root": None, "progress": 0, "total": 0,
                            "files_found": 0, "estimated_total_files": None,
                            "estimated_percent": 0, "comparison_done": 0,
                            "comparison_total": 0, "comparison_percent": 0,
                            "message": "Démarrage du scan FTP (force_refresh)...",
                            "cancel_requested": False,
                        })
                ftp_data = build_ftp_index()
            else:
                ftp_data, ftp_state = ensure_ftp_index(caller="dashboard")
                if ftp_state == "running":
                    app_obj.logger.info("[FTP] Scan en cours, attente...")
                    _FTP_WAIT_TIMEOUT = 600
                    _elapsed = 0
                    while (
                        ftp_index_status.get("running")
                        and ftp_index_status.get("phase") in ("starting", "scanning", "comparing")
                        and _elapsed < _FTP_WAIT_TIMEOUT
                    ):
                        time.sleep(2)
                        _elapsed += 2
                    ftp_data = load_ftp_index() or {"items": []}

            ftp_items_count = len((ftp_data or {}).get("items", []))
            app_obj.logger.info("[FTP] Index chargé : %d items (état: %s)", ftp_items_count, ftp_state if not force_refresh else "force_refresh")

            ftp_ids = _compute_ftp_ids_for_server(app_obj, selected_server_name)
            app_obj.logger.info("[FTP] Serveurs FTP retenus pour '%s': %s", selected_server_name, ftp_ids)

            all_my_items = [item for lib_items in my_sections.values() for item in lib_items]
            indexes = build_indexes(all_my_items)

            comparison_settings = {
                "resolution_filter_mode": app_obj.config.get("RESOLUTION_FILTER_MODE", "none"),
                "use_bitrate": bool(app_obj.config.get("COMPARE_USE_BITRATE", False)),
                "min_bitrate_diff_pct": float(app_obj.config.get("MIN_BITRATE_DIFF_PERCENT", 10)),
                "ignore_transcoded": bool(app_obj.config.get("IGNORE_TRANSCODED_IN_BETTER", False)),
                "show_only_labels": [
                    str(x).strip().lower()
                    for x in app_obj.config.get("SHOW_ONLY_LABELS", [])
                    if str(x).strip()
                ]
            }

            libraries_data = []

            def _item_weight(item):
                if item.get("type") == "show":
                    return max(len(item.get("episodes") or []), 1)
                return 1

            _dashboard_total = sum(
                sum(_item_weight(item) for item in remote_items)
                for lib_title, remote_items in target_sections.items()
                if lib_title in selected_libraries
            )
            _dashboard_done = 0
            _compare_start = time.time()

            with ftp_index_lock:
                ftp_index_status["phase"] = "comparing_dashboard"
                ftp_index_status["comparison_done"] = 0
                ftp_index_status["comparison_total"] = _dashboard_total
                ftp_index_status["comparison_percent"] = 0
                ftp_index_status["current_root"] = None
                ftp_index_status["message"] = "Comparaison Plex..."
                ftp_index_status["started_at"] = _compare_start

            def bump(item):
                nonlocal _dashboard_done
                _dashboard_done += _item_weight(item)
                pct = int((_dashboard_done / _dashboard_total) * 100) if _dashboard_total > 0 else 0
                with ftp_index_lock:
                    ftp_index_status["comparison_done"] = _dashboard_done
                    ftp_index_status["comparison_total"] = _dashboard_total
                    ftp_index_status["comparison_percent"] = pct
                # 33% → 100% range for the loading bar
                _set_bg_status(cache_key, "running", 33 + pct * 67 // 100,
                               f"Comparaison : {pct}%")

            _cancelled = False

            for lib_title, remote_items in target_sections.items():
                if lib_title not in selected_libraries:
                    continue
                if ftp_index_status.get("cancel_requested"):
                    _cancelled = True
                    with ftp_index_lock:
                        ftp_index_status.update({
                            "phase": "cancelled", "running": False, "finished": False,
                            "cancel_requested": False, "message": "Comparaison annulée.",
                        })
                    break

                missing_items = []
                comparison_items = []
                lib_type = remote_items[0]["type"] if remote_items else "unknown"

                for remote_item in remote_items:
                    if ftp_index_status.get("cancel_requested"):
                        _cancelled = True
                        with ftp_index_lock:
                            ftp_index_status.update({
                                "phase": "cancelled", "running": False, "finished": False,
                                "cancel_requested": False, "message": "Comparaison annulée.",
                            })
                        break

                    local_item = find_match(remote_item, indexes)
                    item_type = remote_item.get("type")

                    if is_ignored(remote_item.get("title"), remote_item.get("type", "")):
                        bump(remote_item)
                        continue

                    if not local_item:
                        enriched_missing = dict(remote_item)

                        if item_type == "movie":
                            ftp_matches = find_ftp_matches_for_movie(
                                remote_item.get("title"),
                                remote_item.get("year"),
                                ftp_ids=ftp_ids,
                            )
                            best_match = ftp_matches[0] if ftp_matches else None
                            best_ftp = best_match["item"] if best_match else None

                            enriched_missing["ftp_available"] = bool(best_ftp)
                            enriched_missing["ftp_item"] = best_ftp
                            enriched_missing["ftp_confidence"] = best_match["confidence"] if best_match else None
                            enriched_missing["ftp_variant_type"] = best_match["variant_type"] if best_match else None
                            enriched_missing["sources"] = ["ftp"] if best_ftp else []

                        elif item_type == "show":
                            enriched_eps = []
                            ftp_candidates = []
                            show_title = remote_item.get("title")
                            show_orig_title = remote_item.get("originalTitle") or None

                            from app.services.ftp_alias_service import resolve_alias
                            _api_key = app_obj.config.get("TMDB_API_KEY") or ""
                            _alias = resolve_alias(show_title, media_type="tv", api_key=_api_key)
                            _show_search_titles = list(dict.fromkeys(filter(None, [
                                show_title, _alias, show_orig_title
                            ])))

                            for ep in remote_item.get("episodes", []):
                                ep_season = ep.get("season")
                                ep_episode = ep.get("episode")

                                if ep_season is None or ep_episode is None:
                                    ftp_ep = None
                                else:
                                    ftp_ep = None
                                    for _t in _show_search_titles:
                                        ftp_ep = find_ftp_match_for_episode(_t, ep_season, ep_episode, ftp_ids=ftp_ids)
                                        if ftp_ep:
                                            break

                                ep_copy = dict(ep)
                                ep_copy["ftp_available"] = bool(ftp_ep)
                                ep_copy["ftp_item"] = ftp_ep
                                ep_copy["ftp_confidence"] = 95 if ftp_ep else None
                                ep_copy["ftp_variant_type"] = "episode_match" if ftp_ep else None
                                ep_copy["sources"] = ["ftp"] if ftp_ep else []

                                if ftp_ep:
                                    ftp_candidates.append(ftp_ep)

                                enriched_eps.append(ep_copy)

                            first_ftp_ep = ftp_candidates[0] if ftp_candidates else None

                            enriched_missing["episodes"] = enriched_eps
                            enriched_missing["ftp_available"] = bool(first_ftp_ep)
                            enriched_missing["ftp_item"] = first_ftp_ep
                            enriched_missing["ftp_confidence"] = 95 if first_ftp_ep else None
                            enriched_missing["ftp_variant_type"] = "episode_match" if first_ftp_ep else None
                            enriched_missing["sources"] = ["ftp"] if first_ftp_ep else []

                        else:
                            enriched_missing["ftp_available"] = False
                            enriched_missing["ftp_item"] = None
                            enriched_missing["sources"] = []

                        missing_items.append(enriched_missing)
                        bump(remote_item)
                        continue

                    if item_type == "movie":
                        result = compare_movie(remote_item, local_item, comparison_settings)
                        if result:
                            comparison_items.append(result)
                        bump(remote_item)
                    elif item_type == "show":
                        result = compare_show(remote_item, local_item, comparison_settings)
                        if result:
                            comparison_items.append(result)
                        bump(remote_item)
                    elif item_type == "artist":
                        result = compare_artist_albums(remote_item, local_item)
                        if result:
                            comparison_items.append(result)
                        bump(remote_item)
                    else:
                        bump(remote_item)

                libraries_data.append({
                    "title": lib_title,
                    "type": lib_type,
                    "missing": missing_items,
                    "comparison": comparison_items,
                    "missing_count": len(missing_items),
                    "comparison_count": len(comparison_items),
                })

            _compare_duration = round(time.time() - _compare_start, 1)
            app_obj.logger.info("[DASHBOARD] Comparaison terminée en %ss", _compare_duration)

            with ftp_index_lock:
                ftp_index_status["phase"] = "ready"
                ftp_index_status["comparison_percent"] = 100
                ftp_index_status["running"] = False
                ftp_index_status["finished"] = True
                ftp_index_status["message"] = "FTP prêt"
                ftp_index_status["last_scan_duration_s"] = _compare_duration
                ftp_index_status["last_scan_finished_at"] = __import__("datetime").datetime.now().isoformat(timespec="seconds")
                ftp_index_status["started_at"] = None

            save_cache(cache_key, {"libraries": libraries_data})
            _set_bg_status(cache_key, "done", 100, "Terminé")

        except Exception as e:
            app_obj.logger.exception("Erreur dashboard background")
            with ftp_index_lock:
                ftp_index_status["phase"] = "idle"
                ftp_index_status["running"] = False
                ftp_index_status["finished"] = False
            _set_bg_status(cache_key, "error", 0, str(e))


@dashboard_bp.route("/dashboard/status")
def dashboard_status():
    if not session.get("logged_in"):
        return jsonify({"status": "error", "message": "Non connecté"}), 401
    cache_key = request.args.get("key", "")
    state = _get_bg_status(cache_key)
    return jsonify(state if state else {"status": "unknown"})


@dashboard_bp.route("/dashboard")
def dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("auth.login"))

    plex_token = session.get("plex_token")
    selected_server_name = session.get("selected_server")
    selected_libraries = session.get("selected_libraries", [])

    if not plex_token:
        return redirect(url_for("auth.login"))

    if not selected_server_name:
        if session.get("local_selected_libraries") is not None:
            return redirect(url_for("my_dashboard.my_dashboard"))
        return redirect(url_for("auth.select_server"))

    force_refresh = request.args.get("refresh", "false").lower() == "true"
    refresh_delay = int(current_app.config.get("REFRESH_DELAY_DAYS", 30))

    excluded_libs = [
        str(x).strip().lower()
        for x in current_app.config.get("EXCLUDED_LIBRARIES", [])
        if str(x).strip()
    ]

    excluded_key = "_".join(sorted(excluded_libs)) if excluded_libs else "no_excluded"
    selected_key = "_".join(sorted(selected_libraries)) if selected_libraries else "no_selected"
    cache_key = f"dashboard_{selected_server_name}_{selected_key}_{excluded_key}"

    # ── Debounce : éviter un re-fetch si la page est rechargée < 10s ───────
    last_hit = session.get("dashboard_last_hit", 0)
    now = time.time()
    if now - last_hit < 10 and not force_refresh:
        cache = load_cache(cache_key)
        if cache and cache.get("data", {}).get("libraries") is not None:
            return render_template(
                "dashboard.html",
                libraries=cache["data"]["libraries"],
                resolution_filter_mode=current_app.config.get("RESOLUTION_FILTER_MODE", "none"),
                last_update=cache.get("last_update", ""),
            )
    session["dashboard_last_hit"] = now

    # ── Cache valide → servir directement ───────────────────────────────────
    cache = load_cache(cache_key)
    if not force_refresh and is_cache_valid(cache, refresh_delay):
        return render_template(
            "dashboard.html",
            libraries=cache["data"]["libraries"],
            resolution_filter_mode=current_app.config.get("RESOLUTION_FILTER_MODE", "none"),
            last_update=cache.get("last_update", ""),
        )

    # ── Vérifier si un calcul background est déjà en cours ou terminé ───────
    comp = _get_bg_status(cache_key)

    if comp.get("status") == "running":
        # Calcul en cours → afficher page de chargement
        return render_template("dashboard.html",
                               loading=True,
                               cache_key=cache_key,
                               loading_message=comp.get("message", "Calcul en cours..."),
                               loading_percent=comp.get("percent", 0))

    if comp.get("status") == "done":
        # Calcul terminé → charger le cache (devrait être prêt)
        cache = load_cache(cache_key)
        if cache and cache.get("data", {}).get("libraries") is not None:
            _clear_bg_status(cache_key)
            return render_template(
                "dashboard.html",
                libraries=cache["data"]["libraries"],
                resolution_filter_mode=current_app.config.get("RESOLUTION_FILTER_MODE", "none"),
                last_update=cache.get("last_update", ""),
            )

    if comp.get("status") in ("error", "dead"):
        err_msg = comp.get("message", "Erreur inconnue")
        _clear_bg_status(cache_key)
        return render_template("error.html",
            code=500, icon="💥",
            message="Erreur lors de la comparaison.",
            detail=err_msg), 500

    # ── Lancer le calcul en background ──────────────────────────────────────
    _set_bg_status(cache_key, "running", 0, "Démarrage...")
    app_obj = current_app._get_current_object()
    t = threading.Thread(
        target=_compute_dashboard_bg,
        args=(app_obj, cache_key, plex_token, selected_server_name,
              selected_libraries, excluded_libs, force_refresh),
        daemon=True,
    )
    t.start()

    return render_template("dashboard.html",
                           loading=True,
                           cache_key=cache_key,
                           loading_message="Démarrage de la comparaison...",
                           loading_percent=0)
