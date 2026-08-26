import json
import logging
import time
import threading

from flask import (
    Blueprint,
    render_template,
    session,
    redirect,
    url_for,
    request,
    jsonify,
    Response,
    current_app,
)
from plexapi.myplex import MyPlexAccount

from ..services.plex_label_service import (
    STREAMING_PROVIDERS_CA,
    get_job_status,
    start_label_job,
)
from ..services.rename_scene_service import (
    get_rename_job_status,
    start_rename_job,
    MEDIAINFO_AVAILABLE,
    FFMPEG_AVAILABLE,
)
from ..services.multi_audio_service import (
    get_multi_audio_status,
    start_multi_audio_job,
    check_multi_audio_deps,
    _parse_timecode,
)
from ..services.spotify_harvester_service import (
    get_spotify_job_status,
    start_spotify_job,
    check_spotdl_available,
)

logger = logging.getLogger(__name__)
tools_bp = Blueprint("tools", __name__)


def _get_local_libraries(plex_token):
    account = MyPlexAccount(token=plex_token)
    server = next(
        (s for s in account.resources()
         if s.provides == "server" and getattr(s, "owned", False)),
        None,
    )
    if not server:
        return []
    plex = server.connect()
    libs = []
    for section in plex.library.sections():
        if section.type in ("movie", "show"):
            libs.append({
                "name": section.title,
                "type": section.type,
            })
    return libs


@tools_bp.route("/tools")
def tools_page():
    if not session.get("logged_in"):
        return redirect(url_for("auth.login"))

    libraries = []
    error = None
    try:
        libraries = _get_local_libraries(session["plex_token"])
    except Exception as e:
        error = str(e)
        logger.warning(f"[Tools] impossible de lister les bibliothèques: {e}")

    return render_template(
        "tools.html",
        libraries=libraries,
        providers=STREAMING_PROVIDERS_CA,
        job_status=get_job_status(),
        rename_status=get_rename_job_status(),
        multi_audio_status=get_multi_audio_status(),
        multi_audio_deps=check_multi_audio_deps(),
        mediainfo_available=MEDIAINFO_AVAILABLE,
        ffmpeg_available=FFMPEG_AVAILABLE,
        spotify_status=get_spotify_job_status(),
        spotdl_available=check_spotdl_available(),
        spotify_playlists=current_app.config.get("SPOTIFY_PLAYLISTS", []),
        spotify_output_dir=current_app.config.get("SPOTIFY_OUTPUT_DIR", ""),
        error=error,
        active_tool=request.args.get("tool", "streaming"),
    )


@tools_bp.route("/tools/label/run", methods=["POST"])
def label_run():
    if not session.get("logged_in"):
        return jsonify({"error": "non authentifié"}), 401

    status = get_job_status()
    if status["running"]:
        return jsonify({"error": "job déjà en cours"}), 409

    data = request.get_json(force=True)
    library_names = data.get("libraries", [])
    watch_region = data.get("watch_region", "CA")
    selected_ids = [int(p) for p in data.get("providers", list(STREAMING_PROVIDERS_CA.keys()))]

    if not library_names:
        return jsonify({"error": "aucune bibliothèque sélectionnée"}), 400

    providers = {pid: name for pid, name in STREAMING_PROVIDERS_CA.items()
                 if pid in selected_ids}

    # Résoudre les media_types
    library_configs = []
    try:
        libs = _get_local_libraries(session["plex_token"])
        type_map = {lib["name"]: "tv" if lib["type"] == "show" else "movie" for lib in libs}
        for name in library_names:
            mt = type_map.get(name, "movie")
            library_configs.append((name, mt))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    api_key = current_app.config.get("TMDB_API_KEY", "")
    start_label_job(session["plex_token"], api_key, library_configs, watch_region, providers)
    return jsonify({"ok": True})


@tools_bp.route("/tools/label/status")
def label_status():
    if not session.get("logged_in"):
        return jsonify({"error": "non authentifié"}), 401
    return jsonify(get_job_status())


@tools_bp.route("/tools/label/stream")
def label_stream():
    if not session.get("logged_in"):
        return redirect(url_for("auth.login"))

    def generate():
        sent = 0
        while True:
            status = get_job_status()
            log = status.get("log", [])

            while sent < len(log):
                yield f"data: {json.dumps({'type': 'log', 'msg': log[sent]})}\n\n"
                sent += 1

            yield f"data: {json.dumps({'type': 'status', 'running': status['running'], 'done': status['done'], 'total': status['total'], 'processed': status['processed'], 'labeled': status['labeled'], 'skipped': status['skipped'], 'errors': status['errors']})}\n\n"

            if status["done"] and sent >= len(log):
                yield f"data: {json.dumps({'type': 'done'})}\n\n"
                break

            time.sleep(1)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─── Spotify Harvester ──────────────────────────────────────────────────────

@tools_bp.route("/tools/spotify/run", methods=["POST"])
def spotify_run():
    if not session.get("logged_in"):
        return jsonify({"error": "non authentifié"}), 401

    status = get_spotify_job_status()
    if status["running"]:
        return jsonify({"error": "job déjà en cours"}), 409

    if not check_spotdl_available():
        return jsonify({"error": "'spotdl' introuvable — rebuild l'image Docker."}), 400

    cfg = current_app.config
    playlists = cfg.get("SPOTIFY_PLAYLISTS") or []
    if not playlists:
        return jsonify({"error": "aucune playlist configurée (page Config > Musique)"}), 400

    output_dir = cfg.get("SPOTIFY_OUTPUT_DIR", "")
    if not output_dir:
        return jsonify({"error": "dossier de destination non configuré (page Config > Musique)"}), 400

    data = request.get_json(silent=True) or {}

    started = start_spotify_job(
        playlists=playlists,
        output_dir=output_dir,
        audio_format=cfg.get("SPOTIFY_FORMAT", "mp3"),
        bitrate=cfg.get("SPOTIFY_BITRATE", "auto"),
        threads=cfg.get("SPOTIFY_THREADS", 4),
        client_id=cfg.get("SPOTIFY_CLIENT_ID", ""),
        client_secret=cfg.get("SPOTIFY_CLIENT_SECRET", ""),
        cookie_file=cfg.get("SPOTIFY_COOKIE_FILE", ""),
        max_retries=cfg.get("SPOTIFY_MAX_RETRIES", 2),
        dry_run=bool(data.get("dry_run", False)),
        user_auth=cfg.get("SPOTIFY_USER_AUTH", False),
    )
    if not started:
        return jsonify({"error": "job déjà en cours"}), 409
    return jsonify({"ok": True})


@tools_bp.route("/tools/spotify/status")
def spotify_status():
    if not session.get("logged_in"):
        return jsonify({"error": "non authentifié"}), 401
    return jsonify(get_spotify_job_status())


@tools_bp.route("/tools/spotify/stream")
def spotify_stream():
    if not session.get("logged_in"):
        return redirect(url_for("auth.login"))

    def generate():
        sent = 0
        while True:
            status = get_spotify_job_status()
            log = status.get("log", [])

            while sent < len(log):
                yield f"data: {json.dumps({'type': 'log', 'msg': log[sent]})}\n\n"
                sent += 1

            yield f"data: {json.dumps({'type': 'status', 'running': status['running'], 'done': status['done'], 'total': status['total'], 'processed': status['processed'], 'success': status['success'], 'failed': status['failed']})}\n\n"

            if status["done"] and sent >= len(log):
                yield f"data: {json.dumps({'type': 'done'})}\n\n"
                break

            time.sleep(1)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─── Rename Scene ─────────────────────────────────────────────────────────────

@tools_bp.route("/tools/rename/run", methods=["POST"])
def rename_run():
    if not session.get("logged_in"):
        return jsonify({"error": "non authentifié"}), 401

    status = get_rename_job_status()
    if status["running"]:
        return jsonify({"error": "job déjà en cours"}), 409

    data = request.get_json(force=True)
    path = (data.get("path") or "").strip()
    if not path:
        return jsonify({"error": "chemin manquant"}), 400

    api_key = current_app.config.get("TMDB_API_KEY", "")

    start_rename_job(
        path=path,
        tmdb_api_key=api_key,
        dry_run=bool(data.get("dry_run", True)),
        recursive=bool(data.get("recursive", False)),
        no_justwatch=bool(data.get("no_justwatch", False)),
        service=data.get("service") or None,
        source=data.get("source") or None,
        resolution=data.get("resolution") or None,
        vcodec=data.get("vcodec") or None,
        acodec=data.get("acodec") or None,
        reprocess=bool(data.get("reprocess", False)),
        force=bool(data.get("force", False)),
        team=data.get("team") or None,
        country=data.get("country") or "CA",
        tmdb_id_override=data.get("tmdb_id") or None,
        name_filter=data.get("name_filter") or None,
        validate=bool(data.get("validate", False)),
        corrupted_dir=(data.get("corrupted_dir") or "").strip() or None,
        output_dir=(data.get("output_dir") or "").strip() or None,
    )
    return jsonify({"ok": True})


@tools_bp.route("/tools/rename/status")
def rename_status():
    if not session.get("logged_in"):
        return jsonify({"error": "non authentifié"}), 401
    return jsonify(get_rename_job_status())


@tools_bp.route("/tools/rename/stream")
def rename_stream():
    if not session.get("logged_in"):
        return redirect(url_for("auth.login"))

    def generate():
        sent = 0
        while True:
            status = get_rename_job_status()
            log = status.get("log", [])

            while sent < len(log):
                yield f"data: {json.dumps({'type': 'log', 'msg': log[sent]})}\n\n"
                sent += 1

            yield f"data: {json.dumps({'type': 'status', 'running': status['running'], 'done': status['done'], 'total': status['total'], 'processed': status['processed'], 'renamed': status['renamed'], 'already_ok': status['already_ok'], 'errors': status['errors'], 'corrupted': status['corrupted'], 'unidentified': status['unidentified']})}\n\n"

            if status["done"] and sent >= len(log):
                yield f"data: {json.dumps({'type': 'done'})}\n\n"
                break

            time.sleep(1)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─── Multi Audio Merger ───────────────────────────────────────────────────────

@tools_bp.route("/tools/multi-audio/run", methods=["POST"])
def multi_audio_run():
    if not session.get("logged_in"):
        return jsonify({"error": "non authentifié"}), 401

    status = get_multi_audio_status()
    if status["running"]:
        return jsonify({"error": "job déjà en cours"}), 409

    data = request.get_json(force=True)
    mode = data.get("mode", "auto")

    if mode == "manual":
        file_ref = (data.get("file_ref") or "").strip()
        file_sec = (data.get("file_sec") or "").strip()
        if not file_ref or not file_sec:
            return jsonify({"error": "les deux chemins de fichiers sont requis"}), 400
    else:
        directory = (data.get("directory") or "").strip()
        if not directory:
            return jsonify({"error": "chemin du dossier manquant"}), 400
        file_ref = file_sec = ""

    # Optional segmented-correction params (manual mode only)
    raw_sync_points = data.get("sync_points") or []
    sync_points = None
    if mode == "manual" and raw_sync_points:
        sync_points = []
        for pt in raw_sync_points:
            try:
                sync_points.append({
                    "en": _parse_timecode(str(pt.get("en", "0"))),
                    "fr": _parse_timecode(str(pt.get("fr", "0"))),
                })
            except (ValueError, TypeError):
                pass
        if not sync_points:
            sync_points = None

    prev_offset_ms = float(data.get("prev_offset_ms") or 0)
    prev_speed = float(data.get("prev_speed") or 1.0)
    hint_offset_ms = float(data.get("hint_offset_ms") or 0)
    hint_atempo = float(data.get("hint_atempo") or 0)

    start_multi_audio_job(
        directory=data.get("directory", ""),
        dry_run=bool(data.get("dry_run", True)),
        recursive=bool(data.get("recursive", False)),
        preferred_ref_lang=data.get("ref_lang", ""),
        mode=mode,
        file_ref=file_ref,
        file_sec=file_sec,
        lang_ref=data.get("lang_ref", ""),
        lang_sec=data.get("lang_sec", ""),
        sync_points=sync_points,
        prev_offset_ms=prev_offset_ms,
        prev_speed=prev_speed,
        tmdb_api_key=current_app.config.get("TMDB_API_KEY", ""),
        hint_offset_ms=hint_offset_ms,
        hint_atempo=hint_atempo,
    )
    return jsonify({"ok": True})


@tools_bp.route("/tools/multi-audio/status")
def multi_audio_status_route():
    if not session.get("logged_in"):
        return jsonify({"error": "non authentifié"}), 401
    return jsonify(get_multi_audio_status())


@tools_bp.route("/tools/multi-audio/stream")
def multi_audio_stream():
    if not session.get("logged_in"):
        return redirect(url_for("auth.login"))

    def generate():
        sent = 0
        while True:
            status = get_multi_audio_status()
            log = status.get("log", [])

            while sent < len(log):
                yield f"data: {json.dumps({'type': 'log', 'msg': log[sent]})}\n\n"
                sent += 1

            yield f"data: {json.dumps({'type': 'status', 'running': status['running'], 'done': status['done'], 'total': status['total'], 'processed': status['processed'], 'success': status['success'], 'failed': status['failed']})}\n\n"

            if status["done"] and sent >= len(log):
                yield f"data: {json.dumps({'type': 'done'})}\n\n"
                break

            time.sleep(1)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
