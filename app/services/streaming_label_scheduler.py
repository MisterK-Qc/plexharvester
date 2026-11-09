"""
streaming_label_scheduler.py — Vérification automatique des étiquettes de service streaming.

Tourne dans un thread daemon. À l'heure configurée (STREAMING_LABEL_AUTO_SYNC_TIME),
lance le job d'étiquetage sur toutes les bibliothèques films/séries du serveur local.

plex_label_service.py ignore désormais tout item qui porte déjà une étiquette de
service streaming (posée une fois pour toutes), donc ce job quotidien ne fait
réellement quelque chose que pour les items nouvellement ajoutés à la bibliothèque.
"""
import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

_scheduler_thread: threading.Thread | None = None
_stop_event = threading.Event()


def start_streaming_label_scheduler(app):
    """Démarre le thread de vérification automatique (idempotent)."""
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _stop_event.clear()
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        args=(app,),
        daemon=True,
        name="streaming-label-scheduler",
    )
    _scheduler_thread.start()
    logger.info("[STREAMING LABEL SCHEDULER] Thread démarré")


def _scheduler_loop(app):
    last_run_date   = None
    _heartbeat_tick = 0  # log de présence toutes les ~10 min (20 × 30s)

    while not _stop_event.is_set():
        try:
            with app.app_context():
                from .config_service import load_config
                cfg = load_config()

                enabled     = cfg.get("STREAMING_LABEL_AUTO_SYNC", False)
                sync_time   = str(cfg.get("STREAMING_LABEL_AUTO_SYNC_TIME", "00:00") or "00:00")
                plex_token  = str(cfg.get("PLEX_TOKEN_SCHEDULER", "") or "")
                api_key     = str(cfg.get("TMDB_API_KEY", "") or "")

                now   = datetime.now()
                today = now.date()

                # Heartbeat toutes les ~10 minutes
                _heartbeat_tick += 1
                if _heartbeat_tick >= 20:
                    _heartbeat_tick = 0
                    if enabled and plex_token and api_key:
                        logger.debug(
                            "[STREAMING LABEL SCHEDULER] Actif — heure configurée : %s, heure actuelle : %02d:%02d",
                            sync_time, now.hour, now.minute,
                        )
                    elif enabled and not plex_token:
                        logger.warning(
                            "[STREAMING LABEL SCHEDULER] Activé mais token Plex manquant "
                            "(reconnecte-toi ou visite la page Config pour le sauvegarder)"
                        )
                    elif enabled and not api_key:
                        logger.warning("[STREAMING LABEL SCHEDULER] Activé mais clé API TMDb manquante")
                    # Si disabled : pas de log (comportement attendu)

                if enabled and plex_token and api_key:
                    try:
                        h, m = map(int, sync_time.split(":"))
                    except Exception:
                        logger.warning("[STREAMING LABEL SCHEDULER] Format d'heure invalide : '%s'", sync_time)
                        h, m = 0, 0

                    if now.hour == h and now.minute == m:
                        if last_run_date == today:
                            logger.debug(
                                "[STREAMING LABEL SCHEDULER] Heure %s atteinte mais déjà exécuté aujourd'hui",
                                sync_time,
                            )
                        else:
                            last_run_date = today
                            logger.info("[STREAMING LABEL SCHEDULER] Lancement à %s", sync_time)
                            try:
                                _run_label_job(plex_token, api_key)
                            except Exception:
                                logger.exception("[STREAMING LABEL SCHEDULER] Erreur")

        except Exception:
            logger.exception("[STREAMING LABEL SCHEDULER] Erreur dans la boucle principale")

        _stop_event.wait(30)  # vérifie toutes les 30 secondes


def _run_label_job(plex_token: str, api_key: str):
    from plexapi.myplex import MyPlexAccount
    from .plex_label_service import STREAMING_PROVIDERS_CA, get_job_status, start_label_job

    status = get_job_status()
    if status["running"]:
        logger.info("[STREAMING LABEL SCHEDULER] Job d'étiquetage déjà en cours, run ignoré")
        return

    account = MyPlexAccount(token=plex_token)
    server = next(
        (s for s in account.resources()
         if s.provides == "server" and getattr(s, "owned", False)),
        None,
    )
    if not server:
        logger.warning("[STREAMING LABEL SCHEDULER] Serveur Plex local introuvable")
        return

    plex = server.connect()
    library_configs = []
    for section in plex.library.sections():
        if section.type == "movie":
            library_configs.append((section.title, "movie"))
        elif section.type == "show":
            library_configs.append((section.title, "tv"))

    if not library_configs:
        logger.info("[STREAMING LABEL SCHEDULER] Aucune bibliothèque film/série trouvée")
        return

    start_label_job(plex_token, api_key, library_configs, "CA", dict(STREAMING_PROVIDERS_CA))
    logger.info(
        "[STREAMING LABEL SCHEDULER] Job lancé sur %d bibliothèque(s)",
        len(library_configs),
    )
