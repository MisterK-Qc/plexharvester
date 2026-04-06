from flask import Blueprint, current_app, render_template, request, session, redirect, url_for, Response, jsonify
from plexapi.myplex import MyPlexAccount
from collections import deque
from logging.handlers import TimedRotatingFileHandler
import os
import time
import logging
import threading

_config_save_lock = threading.Lock()
from app.config_paths import LOG_DIR

from ..services.config_service import build_config_from_form, save_config
from .. import i18n as _i18n_module
from ..services.ftp_service import get_ftp_client
from ..services.ftp_index_service import build_ftp_index
from ..services.ftp_alias_service import (
    load_aliases, save_manual_aliases_from_form,
    promote_alias, delete_alias
)

config_bp = Blueprint("config", __name__)

@config_bp.route("/config", methods=["GET", "POST"])
def config_page():
    if not session.get("logged_in"):
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": "Non connecté"}), 401
        return redirect(url_for("auth.login"))

    success_message = None
    plex_libraries = []
    plex_remote_servers = []

    plex_token = session.get("plex_token")
    if plex_token:
        try:
            account = MyPlexAccount(token=plex_token)
            resources = account.resources()
            my_server_resource = next(
                (s for s in resources if s.provides == "server" and s.owned),
                None
            )
            if my_server_resource:
                server = my_server_resource.connect(timeout=30)
                plex_libraries = [
                    {"title": lib.title, "type": lib.type}
                    for lib in server.library.sections()
                ]
            plex_remote_servers = sorted([
                r.name for r in resources
                if "server" in (r.provides or "") and not getattr(r, "owned", False)
            ])
        except Exception:
            plex_libraries = []
            plex_remote_servers = []

    if request.method == "POST":
        try:
            cfg = build_config_from_form(request.form)
            cfg = save_config(cfg)
            current_app.config.update(cfg)
            _i18n_module._cache.clear()

            keys = request.form.getlist("alias_keys[]")
            values = request.form.getlist("alias_values[]")
            save_manual_aliases_from_form(list(zip(keys, values)))

            root_logger = logging.getLogger()
            log_level = getattr(
                logging,
                current_app.config.get("LOG_LEVEL", "INFO").upper(),
                logging.INFO
            )
            root_logger.setLevel(log_level)
            for handler in root_logger.handlers:
                handler.setLevel(log_level)

            # Ne recréer le handler fichier que si backupCount a changé
            retention_days = int(current_app.config.get("LOG_RETENTION_DAYS", 7))
            existing = next(
                (h for h in root_logger.handlers if isinstance(h, TimedRotatingFileHandler)),
                None
            )
            if existing is None or existing.backupCount != retention_days:
                log_file_path = os.path.join(LOG_DIR, "app.log")
                formatter = logging.Formatter("%(asctime)s - %(levelname)-8s - %(name)s - %(message)s")
                file_handler = TimedRotatingFileHandler(
                    log_file_path,
                    when="midnight",
                    interval=1,
                    backupCount=retention_days,
                    encoding="utf-8",
                    utc=False
                )
                file_handler.suffix = "%Y-%m-%d"
                file_handler.setLevel(log_level)
                file_handler.setFormatter(formatter)
                if existing:
                    root_logger.removeHandler(existing)
                    try:
                        existing.close()
                    except Exception:
                        pass
                root_logger.addHandler(file_handler)

            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"ok": True})

            success_message = ""

        except Exception as e:
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({
                    "ok": False,
                    "message": f"Erreur de sauvegarde: {e}"
                }), 500
            raise

    aliases = load_aliases()

    return render_template(
        "config.html",
        config=current_app.config,
        success_message=success_message,
        plex_libraries=plex_libraries,
        plex_remote_servers=plex_remote_servers,
        is_logged_in=True,
        ftp_aliases=aliases,
    )

@config_bp.route("/config/test_ftp", methods=["POST"])
def test_ftp():
    if not session.get("logged_in"):
        return jsonify({"ok": False, "message": "Non connecté"}), 401

    try:
        form = request.form

        host = (form.get("ftp_host") or "").strip()
        port_raw = (form.get("ftp_port") or "21").strip()
        username = (form.get("ftp_user") or "").strip()
        password = form.get("ftp_pass") or ""
        use_tls = "ftp_tls" in form
        passive = "ftp_passive" in form

        if not host:
            return jsonify({"ok": False, "message": "Hôte FTP vide."}), 400

        if host.startswith("ftp://") or host.startswith("ftps://"):
            return jsonify({
                "ok": False,
                "message": "Entre seulement le nom d’hôte, sans ftp:// ou ftps://"
            }), 400

        try:
            port = int(port_raw)
        except ValueError:
            return jsonify({"ok": False, "message": "Port FTP invalide."}), 400

        ftp = get_ftp_client(
            host=host,
            port=port,
            username=username,
            password=password,
            use_tls=use_tls,
            passive=passive,
        )

        try:
            ftp.quit()
        except Exception:
            pass

        return jsonify({
            "ok": True,
            "message": f"Connexion FTP réussie vers {host}:{port}."
        })

    except Exception as e:
        current_app.logger.exception("Échec test FTP")
        return jsonify({
            "ok": False,
            "message": f"Erreur FTP: {e}"
        }), 500

@config_bp.route("/config/reindex_ftp", methods=["POST"])
def reindex_ftp():
    if not session.get("logged_in"):
        return jsonify({"ok": False, "message": "Non connecté"}), 401

    try:
        data = build_ftp_index()
        return jsonify({
            "ok": True,
            "message": f"Index FTP reconstruit ({len(data.get('items', []))} fichiers).",
            "count": len(data.get("items", [])),
            "generated_at": data.get("generated_at")
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "message": f"Erreur d’indexation FTP: {e}"
        }), 500

@config_bp.route("/config/alias/promote", methods=["POST"])
def alias_promote():
    if not session.get("logged_in"):
        return jsonify({"ok": False}), 401
    title = (request.get_json(silent=True) or {}).get("title", "")
    ok = promote_alias(title)
    return jsonify({"ok": ok})


@config_bp.route("/config/alias/delete", methods=["POST"])
def alias_delete():
    if not session.get("logged_in"):
        return jsonify({"ok": False}), 401
    title = (request.get_json(silent=True) or {}).get("title", "")
    ok = delete_alias(title)
    return jsonify({"ok": ok})


@config_bp.route("/logs_tail")
def logs_tail():
    if not session.get("logged_in"):
        return jsonify({"lines": []}), 401

    log_dir = LOG_DIR
    base_log = os.path.join(log_dir, "app.log")

    try:
        candidates = []

        if os.path.exists(base_log):
            candidates.append(base_log)

        # Ajouter les logs rotatés
        for name in os.listdir(log_dir):
            if name.startswith("app.log."):
                candidates.append(os.path.join(log_dir, name))

        # Trier par date de modification
        candidates = sorted(candidates, key=lambda p: os.path.getmtime(p))

        all_lines = deque(maxlen=100)

        for path in candidates:
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        all_lines.append(line.rstrip("\n"))
            except Exception:
                continue

        return jsonify({"lines": list(all_lines)})

    except Exception:
        return jsonify({"lines": []})

@config_bp.route("/logs_stream")
def logs_stream():
    if not session.get("logged_in"):
        return redirect(url_for("auth.login"))

    log_file = os.path.join(LOG_DIR, "app.log")

    def generate():
        if not os.path.exists(log_file):
            open(log_file, "a", encoding="utf-8").close()

        with open(log_file, "r", encoding="utf-8", errors="replace") as f:
            f.seek(0, os.SEEK_END)

            while True:
                line = f.readline()
                if line:
                    try:
                        yield f"data: {line.rstrip()}\n\n"
                    except GeneratorExit:
                        return
                else:
                    try:
                        yield ":\n\n"  # SSE keepalive — détecte la déconnexion client
                    except GeneratorExit:
                        return
                    time.sleep(0.5)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )