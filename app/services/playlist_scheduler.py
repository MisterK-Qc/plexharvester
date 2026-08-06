"""
playlist_scheduler.py — Synchronisation automatique des playlists Plex.

Tourne dans un thread daemon. À l'heure configurée (PLAYLIST_AUTO_SYNC_TIME),
re-vérifie tous les placeholders des playlists sauvegardées contre Plex.
Si de nouveaux items sont disponibles → met à jour le store + re-push vers Plex.
"""
import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

_scheduler_thread: threading.Thread | None = None
_stop_event = threading.Event()


def start_playlist_scheduler(app):
    """Démarre le thread de synchronisation automatique (idempotent)."""
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _stop_event.clear()
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        args=(app,),
        daemon=True,
        name="playlist-scheduler",
    )
    _scheduler_thread.start()
    logger.info("[PLAYLIST SCHEDULER] Thread démarré")


def _scheduler_loop(app):
    last_run_date   = None
    _heartbeat_tick = 0  # log de présence toutes les ~10 min (20 × 30s)

    while not _stop_event.is_set():
        try:
            with app.app_context():
                from .config_service import load_config
                cfg = load_config()

                enabled     = cfg.get("PLAYLIST_AUTO_SYNC", False)
                sync_time   = str(cfg.get("PLAYLIST_AUTO_SYNC_TIME", "00:00") or "00:00")
                plex_token  = str(cfg.get("PLEX_TOKEN_SCHEDULER", "") or "")

                now   = datetime.now()
                today = now.date()

                # Heartbeat toutes les ~10 minutes
                _heartbeat_tick += 1
                if _heartbeat_tick >= 20:
                    _heartbeat_tick = 0
                    if enabled and plex_token:
                        logger.debug(
                            "[PLAYLIST SCHEDULER] Actif — heure configurée : %s, heure actuelle : %02d:%02d",
                            sync_time, now.hour, now.minute,
                        )
                    elif enabled and not plex_token:
                        logger.warning(
                            "[PLAYLIST SCHEDULER] Activé mais token Plex manquant "
                            "(reconnecte-toi ou visite la page Config pour le sauvegarder)"
                        )
                    # Si disabled : pas de log (comportement attendu)

                if enabled and plex_token:
                    try:
                        h, m = map(int, sync_time.split(":"))
                    except Exception:
                        logger.warning("[PLAYLIST SCHEDULER] Format d'heure invalide : '%s'", sync_time)
                        h, m = 0, 0

                    if now.hour == h and now.minute == m:
                        if last_run_date == today:
                            logger.debug(
                                "[PLAYLIST SCHEDULER] Heure %s atteinte mais déjà exécuté aujourd'hui",
                                sync_time,
                            )
                        else:
                            last_run_date = today
                            logger.info("[PLAYLIST SCHEDULER] Lancement sync à %s", sync_time)
                            try:
                                _run_sync(plex_token)
                            except Exception:
                                logger.exception("[PLAYLIST SCHEDULER] Erreur sync")

        except Exception:
            logger.exception("[PLAYLIST SCHEDULER] Erreur dans la boucle principale")

        _stop_event.wait(30)  # vérifie toutes les 30 secondes


def _run_sync(plex_token: str):
    from .config_service import load_config
    from .playlist_store_service import list_playlists, get_playlist, upsert_playlist
    from .playlist_service import (
        _get_local_server, _build_guid_index,
        _guid_lookup, _find_by_title, _find_episode, _find_season,
        create_or_update_playlist,
    )

    cfg             = load_config()
    auto_push_sync  = cfg.get("PLAYLIST_AUTO_PUSH_SYNC", False)

    all_summaries = list_playlists()
    if not all_summaries:
        logger.info("[PLAYLIST SCHEDULER] Aucune playlist sauvegardée")
        return

    to_process  = [s for s in all_summaries if s.get("pending", 0) > 0]
    needs_plex  = bool(to_process) or auto_push_sync

    if not needs_plex:
        logger.info("[PLAYLIST SCHEDULER] Aucun placeholder à re-vérifier, auto-push désactivé")
        return

    # Connexion Plex + index GUID (une seule fois pour toutes les playlists)
    plex       = _get_local_server(plex_token)
    guid_index = _build_guid_index(plex)

    # ── Phase 1 : résolution des placeholders ────────────────────────────────
    total_resolved = 0

    for summary in to_process:
        pid      = summary["id"]
        playlist = get_playlist(pid)
        if not playlist:
            continue

        items           = [dict(it) for it in playlist.get("items", [])]
        newly_available = 0

        for it in items:
            if it.get("plex_key"):
                continue

            media_type = it.get("type", "movie")
            plex_item  = None

            logger.debug(
                "[PLAYLIST SCHEDULER] Recherche '%s' (%s, tmdb=%s imdb=%s tvdb=%s) dans '%s'",
                it.get("title"), media_type,
                it.get("tmdb_id"), it.get("imdb_id"), it.get("tvdb_id"),
                playlist["name"],
            )

            if media_type == "episode":
                plex_item = _find_episode(plex, it, guid_index)
            elif media_type == "season":
                plex_item = _find_season(plex, it, guid_index)
            else:
                plex_item = _guid_lookup(
                    guid_index,
                    tmdb_id=it.get("tmdb_id"),
                    imdb_id=it.get("imdb_id"),
                    tvdb_id=it.get("tvdb_id"),
                )
                if not plex_item:
                    plex_item = _find_by_title(
                        plex,
                        title=it.get("title", ""),
                        year=it.get("year"),
                        media_type=media_type,
                    )

            if plex_item:
                it["plex_key"]   = str(plex_item.ratingKey)
                it["plex_title"] = plex_item.title
                it["available"]  = True
                newly_available += 1
                logger.info("[PLAYLIST SCHEDULER] ✓ '%s' résolu dans '%s'",
                            it.get("title"), playlist["name"])
            else:
                logger.debug("[PLAYLIST SCHEDULER] ✗ '%s' non trouvé dans Plex", it.get("title"))

        if newly_available:
            upsert_playlist(playlist["name"], playlist.get("trakt_url", ""), items)
            _push_playlist(plex, playlist["name"], items, newly_available)
            total_resolved += newly_available

    logger.info("[PLAYLIST SCHEDULER] Sync terminée — %d item(s) résolus", total_resolved)

    # ── Phase 2 : push sync (compare store vs playlist Plex) ─────────────────
    if not auto_push_sync:
        return

    logger.info("[PLAYLIST SCHEDULER] Push sync — vérification de %d playlist(s)", len(all_summaries))

    for summary in all_summaries:
        playlist = get_playlist(summary["id"])
        if not playlist:
            continue

        items           = playlist.get("items", [])
        available_items = [
            it for it in items
            if it.get("plex_key") and it.get("available") and it.get("checked", True)
        ]
        if not available_items:
            continue

        # Nombre de feuilles attendues dans la playlist Plex (épisodes expandés)
        expected_leaf = sum(
            it.get("episode_count") or 1
            for it in available_items
        )

        # Nombre de feuilles actuelles dans la playlist Plex
        try:
            plex_pl   = plex.playlist(playlist["name"])
            plex_leaf = getattr(plex_pl, "leafCount", 0) or 0
        except Exception:
            plex_leaf = 0  # playlist inexistante

        logger.debug(
            "[PLAYLIST SCHEDULER] '%s' : Plex=%d feuilles, store=%d attendues",
            playlist["name"], plex_leaf, expected_leaf,
        )

        if plex_leaf >= expected_leaf:
            continue  # tout est déjà en ordre

        logger.info(
            "[PLAYLIST SCHEDULER] '%s' : %d/%d feuilles dans Plex → re-push",
            playlist["name"], plex_leaf, expected_leaf,
        )
        _push_playlist(plex, playlist["name"], items)


def _push_playlist(plex, name: str, items: list, newly_available: int = 0):
    """Construit la liste d'items Plex et pousse la playlist."""
    from .playlist_service import create_or_update_playlist
    try:
        plex_keys  = [
            it["plex_key"] for it in items
            if it.get("plex_key") and it.get("available") and it.get("checked", True)
        ]
        plex_items = []
        for k in plex_keys:
            obj = plex.fetchItem(int(k))
            if obj.type == "season":
                plex_items.extend(obj.episodes())
            else:
                plex_items.append(obj)

        if plex_items:
            create_or_update_playlist(plex, name, plex_items)
            if newly_available:
                logger.info(
                    "[PLAYLIST SCHEDULER] '%s' : %d nouveau(x), playlist Plex mise à jour (%d éléments)",
                    name, newly_available, len(plex_items),
                )
            else:
                logger.info(
                    "[PLAYLIST SCHEDULER] '%s' : playlist Plex mise à jour (%d éléments)",
                    name, len(plex_items),
                )
    except Exception:
        logger.exception("[PLAYLIST SCHEDULER] Erreur push '%s'", name)
