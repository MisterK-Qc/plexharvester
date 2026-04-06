from flask import Blueprint, request, jsonify, current_app, session

from app.services.ftp_index_service import (
    build_ftp_index,
    ensure_ftp_index,
    load_ftp_index,
    ftp_index_status,
    ftp_index_statuses,
    ftp_index_lock,
)

from app.services.ftp_download_service import (
    create_ftp_download_job,
    cancel_ftp_job,
    get_download_status,
    get_download_status_by_media_key,
    move_ftp_job_up,
    move_ftp_job_down,
    _daily_counter,
    _daily_counter_lock,
)

from app.services.ignore_service import (
    load_ignore_list,
    add_ignore,
    remove_ignore,
)

ftp_bp = Blueprint("ftp", __name__, url_prefix="/ftp")


@ftp_bp.route("/status", methods=["GET"])
def ftp_status():
    return jsonify({
        **ftp_index_status,
        "servers": ftp_index_statuses,
    })


@ftp_bp.route("/reindex", methods=["POST"])
def reindex():
    """
    Lance un rebuild de l'index FTP.
    Body JSON optionnel : { "ftp_id": "ftp_1" } pour ne rebuilder qu'un seul serveur.
    Sans ftp_id → rebuild tous les serveurs activés.
    """
    req_data = request.get_json(silent=True) or {}
    ftp_id = req_data.get("ftp_id")

    # Trouver la config du serveur si ftp_id fourni
    ftp_cfg = None
    if ftp_id:
        servers = current_app.config.get("FTP_SERVERS") or []
        ftp_cfg = next((s for s in servers if s.get("id") == ftp_id), None)
        if not ftp_cfg:
            return jsonify({"success": False, "error": f"Serveur FTP '{ftp_id}' introuvable."}), 404

    with ftp_index_lock:
        if ftp_index_status.get("running"):
            return jsonify({"success": False, "error": "Un scan FTP est déjà en cours."}), 409

        ftp_index_status.update({
            "running": True, "finished": False, "phase": "starting",
            "current_root": None, "progress": 0, "total": 0,
            "files_found": 0, "estimated_total_files": None, "estimated_percent": 0,
            "comparison_done": 0, "comparison_total": 0, "comparison_percent": 0,
            "message": "Démarrage du scan FTP...",
        })

    try:
        data = build_ftp_index(ftp_cfg)
        return jsonify({
            "success": True,
            "count": len(data.get("items", [])),
            "generated_at": data.get("generated_at"),
        })
    except Exception as e:
        with ftp_index_lock:
            ftp_index_status.update({
                "running": False, "finished": False, "phase": "idle",
                "current_root": None, "estimated_percent": None,
                "comparison_percent": 0, "message": f"Erreur index FTP: {e}",
            })
        raise
    

@ftp_bp.route("/cancel_scan", methods=["POST"])
def cancel_scan():
    with ftp_index_lock:
        phase = ftp_index_status.get("phase", "idle")
        is_running = ftp_index_status.get("running", False)
        is_comparing = phase in ("comparing_dashboard",)
        if is_running or is_comparing:
            ftp_index_status["cancel_requested"] = True
            return jsonify({"success": True, "message": "Annulation demandée."})
    return jsonify({"success": False, "message": "Aucun scan en cours."}), 400


@ftp_bp.route("/index_status", methods=["GET"])
def index_status():
    ftp_id = request.args.get("ftp_id") or None
    data = load_ftp_index(ftp_id) or {"items": []}
    return jsonify({
        "success": True,
        "count": len(data.get("items", [])),
        "generated_at": data.get("generated_at"),
        "ftp_id": ftp_id,
    })


@ftp_bp.route("/queue_download", methods=["POST"])
def queue_download():
    data = request.get_json() or {}
    current_app.logger.debug("[QUEUE DOWNLOAD PAYLOAD] %s", data)

    try:
        job_id, queue_position = create_ftp_download_job(
            remote_path=data.get("remote_path"),
            filename=data.get("filename"),
            media_type=data.get("media_type", "movie"),
            media_key=data.get("media_key"),
            ftp_id=data.get("ftp_id") or None,
        )

        return jsonify({
            "success": True,
            "job_id": job_id,
            "status": "queued",
            "queue_position": queue_position,
        })

    except Exception as e:
        current_app.logger.exception("QUEUE DOWNLOAD ERROR")
        return jsonify({
            "success": False,
            "error": str(e),
        }), 400


@ftp_bp.route("/cancel_download", methods=["POST"])
def cancel_download():
    data = request.get_json() or {}
    result = cancel_ftp_job(data.get("job_id"))

    if result.get("success"):
        return jsonify(result)

    return jsonify(result), 400


@ftp_bp.route("/download_status", methods=["GET"])
def download_status():
    job_id = request.args.get("job_id")
    media_key = request.args.get("media_key")

    if job_id:
        data = get_download_status(job_id)
        status_code = 200 if data.get("success") else 404
        return jsonify(data), status_code

    if media_key:
        data = get_download_status_by_media_key(media_key)
        status_code = 200 if data.get("success") else 400
        return jsonify(data), status_code

    return jsonify({
        "success": False,
        "error": "job_id ou media_key manquant",
    }), 400


@ftp_bp.route("/move_up", methods=["POST"])
def move_up():
    data = request.get_json() or {}
    job_id = data.get("job_id")

    if not job_id:
        return jsonify({"error": "job_id manquant"}), 400

    result = move_ftp_job_up(job_id)
    return jsonify(result)


@ftp_bp.route("/move_down", methods=["POST"])
def move_down():
    data = request.get_json() or {}
    job_id = data.get("job_id")

    if not job_id:
        return jsonify({"error": "job_id manquant"}), 400

    result = move_ftp_job_down(job_id)
    return jsonify(result)


@ftp_bp.route("/auto_status", methods=["GET"])
def auto_status():
    if not session.get("logged_in"):
        return jsonify({"ok": False}), 401

    from datetime import datetime
    now = datetime.now()

    auto_enabled = bool(current_app.config.get("AUTO_DOWNLOAD_ENABLED", False))
    skip_days = current_app.config.get("FTP_AUTO_SKIP_DAYS") or []
    start_str = str(current_app.config.get("AUTO_DOWNLOAD_START") or "00:00")
    end_str = str(current_app.config.get("AUTO_DOWNLOAD_END") or "06:00")
    max_per_day = int(current_app.config.get("FTP_AUTO_MAX_PER_DAY") or 0)

    today = now.strftime("%Y-%m-%d")
    with _daily_counter_lock:
        daily_count = _daily_counter["count"] if _daily_counter["date"] == today else 0

    is_skip_day = now.day in skip_days

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
    else:
        in_window = current_min >= start_min or current_min < end_min

    limit_reached = max_per_day > 0 and daily_count >= max_per_day
    active = auto_enabled and in_window and not is_skip_day and not limit_reached

    return jsonify({
        "ok": True,
        "enabled": auto_enabled,
        "active": active,
        "in_window": in_window,
        "is_skip_day": is_skip_day,
        "limit_reached": limit_reached,
        "daily_count": daily_count,
        "max_per_day": max_per_day,
        "window_start": start_str,
        "window_end": end_str,
        "current_time": now.strftime("%H:%M"),
        "skip_days": skip_days,
    })


@ftp_bp.route("/ignore/list", methods=["GET"])
def ignore_list():
    return jsonify({"ok": True, "items": load_ignore_list()})


@ftp_bp.route("/ignore/add", methods=["POST"])
def ignore_add():
    data = request.get_json(silent=True) or {}
    title = (data.get("title") or "").strip()
    item_type = (data.get("type") or "")
    reason = (data.get("reason") or "")
    if not title:
        return jsonify({"ok": False, "error": "title manquant"}), 400
    ok = add_ignore(title, item_type, reason)
    return jsonify({"ok": ok})


@ftp_bp.route("/ignore/remove", methods=["POST"])
def ignore_remove():
    data = request.get_json(silent=True) or {}
    title = (data.get("title") or "").strip()
    if not title:
        return jsonify({"ok": False, "error": "title manquant"}), 400
    ok = remove_ignore(title)
    return jsonify({"ok": ok})