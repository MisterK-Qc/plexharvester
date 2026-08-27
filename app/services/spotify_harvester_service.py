"""
spotify_harvester_service.py — Intégration spotdl pour PlexHarvester.

Télécharge une ou plusieurs playlists Spotify vers une bibliothèque musicale
organisée au format Plex (Artiste/Album/NN - Titre.ext). Adapté du script
autonome spotify_harvester.py pour tourner comme les autres outils de l'app
(job en arrière-plan + suivi de statut/log, déclenché depuis l'UI ou le
scheduler quotidien), avec la configuration lue depuis config.json au lieu
d'un fichier YAML séparé.
"""

import json
import logging
import shutil
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

from app.config_paths import LOG_DIR, SPOTIFY_ARCHIVE_FILE

logger = logging.getLogger(__name__)

# Gabarit de sortie standard Plex Music : Artiste/Album/NN - Titre.ext
# Note : spotdl ne supporte pas les format-specs Python ({track-number:02d}) —
# {track-number} seul est déjà zero-paddé nativement par spotdl (ex: "06").
PLEX_OUTPUT_TEMPLATE = "{artist}/{album}/{track-number} - {title}.{output-ext}"

SUPPORTED_FORMATS = {"mp3", "flac", "m4a", "ogg", "opus", "wav"}

SPOTDL_BINARY = "spotdl"


def check_spotdl_available() -> bool:
    return shutil.which(SPOTDL_BINARY) is not None


# ─── État du job ────────────────────────────────────────────────────────────
_spotify_lock = threading.Lock()
_spotify_status = {
    "running": False,
    "done": False,
    "total": 0,
    "processed": 0,
    "success": 0,
    "failed": 0,
    "log": [],
}


def get_spotify_job_status():
    with _spotify_lock:
        s = dict(_spotify_status)
        s["log"] = list(_spotify_status["log"])
        return s


def _update(**kw):
    with _spotify_lock:
        _spotify_status.update(kw)


def _log(msg):
    logger.info("[SPOTIFY] %s", msg)
    with _spotify_lock:
        _spotify_status["log"].append(msg)
        if len(_spotify_status["log"]) > 500:
            _spotify_status["log"] = _spotify_status["log"][-500:]


def build_command(
    playlist: dict,
    output_dir: str,
    audio_format: str,
    bitrate: str,
    threads: int,
    client_id: str = "",
    client_secret: str = "",
    cookie_file: str = "",
    dry_run: bool = False,
    user_auth: bool = False,
) -> list[str]:
    fmt = (playlist.get("format") or audio_format or "mp3").strip()
    br = (playlist.get("bitrate") or bitrate or "auto").strip()

    output_template = str(Path(output_dir) / PLEX_OUTPUT_TEMPLATE)

    cmd: list[str] = [SPOTDL_BINARY]

    if client_id and client_secret:
        # Sans --use-official-api, spotdl ignore les identifiants fournis et utilise par
        # défaut le client anonyme "SpotipyFree" (limité, cassé par les derniers changements
        # de l'API Spotify — cause de "BaseClientError: Could not get session auth tokens").
        cmd += ["--client-id", client_id, "--client-secret", client_secret, "--use-official-api"]

    # La query (URL) doit suivre immédiatement l'opération "download", avant les options
    # (syntaxe spotdl : `spotdl download <query> --option value ...`).
    cmd += ["download", playlist["url"]]

    # --user-auth déclenche un flow OAuth navigateur interactif (bloquant en headless) —
    # uniquement nécessaire pour les Liked Songs / playlists privées, jamais pour une
    # playlist publique. Ne l'ajouter que si explicitement demandé.
    if user_auth:
        cmd += ["--user-auth"]

    if cookie_file:
        cmd += ["--cookie-file", cookie_file]

    cmd += [
        "--format", fmt,
        "--bitrate", br,
        "--threads", str(max(1, int(threads or 4))),
        "--output", output_template,
        "--overwrite", "skip",  # ne retélécharge pas ce qui existe déjà sur disque
        "--archive", str(SPOTIFY_ARCHIVE_FILE),
        "--print-errors",
    ]

    if dry_run:
        cmd += ["--simple-tui"]

    return cmd


SUBPROCESS_TIMEOUT_S = 7200  # garde-fou : ne devrait jamais être atteint par un téléchargement normal


def _run_playlist(playlist: dict, output_dir, audio_format, bitrate, threads,
                   client_id, client_secret, cookie_file, max_retries, dry_run, user_auth=False) -> dict:
    display_name = playlist.get("name") or playlist["url"]
    cmd = build_command(
        playlist, output_dir, audio_format, bitrate, threads,
        client_id, client_secret, cookie_file, dry_run, user_auth,
    )

    safe_cmd = [
        "***" if cmd[i - 1] == "--client-secret" else tok
        for i, tok in enumerate(cmd)
    ]
    _log(f"Playlist '{display_name}' — commande : {' '.join(safe_cmd)}")

    if dry_run:
        _log(f"[DRY-RUN] '{display_name}' — aucune exécution réelle.")
        return {"name": display_name, "url": playlist["url"], "success": True, "duration_s": 0.0, "error": None, "attempts": 1}

    start = time.monotonic()
    last_error = None

    for attempt in range(1, max_retries + 2):  # essai initial + retries
        proc = None
        try:
            # Popen + lecture ligne par ligne : spotdl peut prendre plusieurs minutes
            # (recherche + téléchargement + conversion par piste) sans jamais échouer —
            # capturer la sortie en direct évite que le job ait l'air figé pendant ce temps.
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1,
            )
            output_lines = []
            deadline = time.monotonic() + SUBPROCESS_TIMEOUT_S
            timed_out = False

            for line in proc.stdout:
                line = line.rstrip()
                if line:
                    output_lines.append(line)
                    _log(f"  {line}")
                if time.monotonic() > deadline:
                    timed_out = True
                    proc.kill()
                    break

            returncode = proc.wait(timeout=30)
            duration = time.monotonic() - start

            if timed_out:
                raise subprocess.TimeoutExpired(cmd, SUBPROCESS_TIMEOUT_S)

            if returncode == 0:
                _log(f"Playlist '{display_name}' terminée avec succès (essai {attempt}, {duration:.1f}s).")
                return {"name": display_name, "url": playlist["url"], "success": True, "duration_s": duration, "error": None, "attempts": attempt}

            last_error = "\n".join(output_lines[-20:])
            _log(f"Playlist '{display_name}' — échec essai {attempt}/{max_retries + 1} (code {returncode}).")

        except subprocess.TimeoutExpired:
            if proc is not None:
                proc.kill()
            last_error = f"Timeout après {SUBPROCESS_TIMEOUT_S}s (processus bloqué, ex: OAuth --user-auth en attente)"
            _log(f"Playlist '{display_name}' — {last_error}")

        except Exception as exc:
            last_error = str(exc)
            _log(f"Playlist '{display_name}' — exception essai {attempt}/{max_retries + 1} : {last_error}")

        if attempt <= max_retries:
            time.sleep(5 * attempt)  # backoff progressif

    duration = time.monotonic() - start
    _log(f"Playlist '{display_name}' — échec définitif après {attempt} essai(s).")
    return {"name": display_name, "url": playlist["url"], "success": False, "duration_s": duration, "error": last_error, "attempts": attempt}


def _write_report(results: list[dict]):
    report_path = Path(LOG_DIR) / f"spotify_rapport_{datetime.now():%Y%m%d_%H%M%S}.json"
    payload = {
        "date": datetime.now().isoformat(),
        "playlists": results,
        "resume": {
            "total": len(results),
            "reussies": sum(1 for r in results if r["success"]),
            "echouees": sum(1 for r in results if not r["success"]),
        },
    }
    try:
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        _log(f"Rapport écrit : {report_path.name} ({payload['resume']['reussies']}/{payload['resume']['total']} réussies)")
    except Exception:
        logger.exception("[SPOTIFY] Erreur écriture rapport")


def _worker(playlists, output_dir, audio_format, bitrate, threads,
            client_id, client_secret, cookie_file, max_retries, dry_run, user_auth=False):
    with _spotify_lock:
        _spotify_status.update({
            "running": True, "done": False,
            "total": len(playlists), "processed": 0,
            "success": 0, "failed": 0, "log": [],
        })

    try:
        if not check_spotdl_available():
            _log("❌ 'spotdl' introuvable dans le PATH — installe-le dans l'image Docker (pip install spotdl).")
            _update(running=False, done=True)
            return

        Path(output_dir).mkdir(parents=True, exist_ok=True)

        results = []
        for playlist in playlists:
            result = _run_playlist(
                playlist, output_dir, audio_format, bitrate, threads,
                client_id, client_secret, cookie_file, max_retries, dry_run, user_auth,
            )
            results.append(result)
            with _spotify_lock:
                _spotify_status["processed"] += 1
                if result["success"]:
                    _spotify_status["success"] += 1
                else:
                    _spotify_status["failed"] += 1

        if not dry_run:
            _write_report(results)

    except Exception:
        logger.exception("[SPOTIFY] Erreur job harvester")
        _log("❌ Erreur inattendue — voir les logs de l'application.")
    finally:
        _update(running=False, done=True)


def start_spotify_job(playlists, output_dir, audio_format="mp3", bitrate="auto", threads=4,
                       client_id="", client_secret="", cookie_file="", max_retries=2,
                       dry_run=False, user_auth=False) -> bool:
    """Démarre un job de téléchargement en arrière-plan. Retourne False si un job tourne déjà."""
    with _spotify_lock:
        if _spotify_status["running"]:
            return False

    t = threading.Thread(
        target=_worker,
        args=(playlists, output_dir, audio_format, bitrate, threads,
              client_id, client_secret, cookie_file, max_retries, dry_run, user_auth),
        daemon=True,
    )
    t.start()
    return True
