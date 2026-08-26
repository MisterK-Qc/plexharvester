"""
spotify_scheduler.py — Lancement automatique quotidien du Spotify Harvester.

Tourne dans un thread daemon. À l'heure configurée (SPOTIFY_AUTO_SYNC_TIME),
si SPOTIFY_AUTO_ENABLED est actif et qu'au moins une playlist est configurée,
lance un job de téléchargement (une fois par jour maximum).
"""
import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

_scheduler_thread: threading.Thread | None = None
_stop_event = threading.Event()


def start_spotify_scheduler(app):
    """Démarre le thread de synchronisation automatique (idempotent)."""
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _stop_event.clear()
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        args=(app,),
        daemon=True,
        name="spotify-scheduler",
    )
    _scheduler_thread.start()
    logger.info("[SPOTIFY SCHEDULER] Thread démarré")


def _scheduler_loop(app):
    last_run_date = None
    _heartbeat_tick = 0  # log de présence toutes les ~10 min (20 × 30s)

    while not _stop_event.is_set():
        try:
            with app.app_context():
                from .config_service import load_config
                cfg = load_config()

                enabled = cfg.get("SPOTIFY_AUTO_ENABLED", False)
                sync_time = str(cfg.get("SPOTIFY_AUTO_SYNC_TIME", "04:00") or "04:00")
                playlists = cfg.get("SPOTIFY_PLAYLISTS") or []

                now = datetime.now()
                today = now.date()

                _heartbeat_tick += 1
                if _heartbeat_tick >= 20:
                    _heartbeat_tick = 0
                    if enabled and playlists:
                        logger.debug(
                            "[SPOTIFY SCHEDULER] Actif — heure configurée : %s, heure actuelle : %02d:%02d",
                            sync_time, now.hour, now.minute,
                        )
                    elif enabled and not playlists:
                        logger.warning(
                            "[SPOTIFY SCHEDULER] Activé mais aucune playlist configurée "
                            "(section Musique de la page Config)."
                        )

                if enabled and playlists:
                    try:
                        h, m = map(int, sync_time.split(":"))
                    except Exception:
                        logger.warning("[SPOTIFY SCHEDULER] Format d'heure invalide : '%s'", sync_time)
                        h, m = 4, 0

                    if now.hour == h and now.minute == m:
                        if last_run_date == today:
                            logger.debug(
                                "[SPOTIFY SCHEDULER] Heure %s atteinte mais déjà exécuté aujourd'hui",
                                sync_time,
                            )
                        else:
                            last_run_date = today
                            logger.info("[SPOTIFY SCHEDULER] Lancement à %s", sync_time)
                            try:
                                _run_auto(cfg)
                            except Exception:
                                logger.exception("[SPOTIFY SCHEDULER] Erreur lancement")

        except Exception:
            logger.exception("[SPOTIFY SCHEDULER] Erreur dans la boucle principale")

        _stop_event.wait(30)  # vérifie toutes les 30 secondes


def _run_auto(cfg):
    from .spotify_harvester_service import start_spotify_job, get_spotify_job_status

    status = get_spotify_job_status()
    if status["running"]:
        logger.info("[SPOTIFY SCHEDULER] Job déjà en cours — passage ignoré")
        return

    started = start_spotify_job(
        playlists=cfg.get("SPOTIFY_PLAYLISTS") or [],
        output_dir=cfg.get("SPOTIFY_OUTPUT_DIR", ""),
        audio_format=cfg.get("SPOTIFY_FORMAT", "mp3"),
        bitrate=cfg.get("SPOTIFY_BITRATE", "auto"),
        threads=cfg.get("SPOTIFY_THREADS", 4),
        client_id=cfg.get("SPOTIFY_CLIENT_ID", ""),
        client_secret=cfg.get("SPOTIFY_CLIENT_SECRET", ""),
        cookie_file=cfg.get("SPOTIFY_COOKIE_FILE", ""),
        max_retries=cfg.get("SPOTIFY_MAX_RETRIES", 2),
        dry_run=False,
    )
    if not started:
        logger.info("[SPOTIFY SCHEDULER] Job déjà en cours (race) — passage ignoré")
