import base64
import logging
import os
import re
import secrets
from logging.handlers import TimedRotatingFileHandler

from flask import Flask, render_template, session

from .filters import register_filters
from .i18n import translate
from .services.config_service import load_config
from .services.ftp_download_service import start_ftp_queue_worker
from .routes.ftp_routes import ftp_bp
from .config_paths import LOG_DIR


def _resolve_secret_key() -> str:
    key = os.environ.get("SECRET_KEY", "").strip()
    if key:
        return key
    config_dir = os.environ.get("PLEX_COMPARE_CONFIG_DIR", "/config")
    key_file = os.path.join(config_dir, "secret_key.txt")
    if os.path.exists(key_file):
        try:
            stored = open(key_file).read().strip()
            if stored:
                return stored
        except Exception:
            pass
    key = secrets.token_hex(32)
    os.makedirs(config_dir, exist_ok=True)
    with open(key_file, "w") as f:
        f.write(key)
    return key


def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = _resolve_secret_key()

    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

    # Charger la config une seule fois
    app.config.update(load_config())
    app.config.setdefault("LANGUAGE", "fr")
    app.config.setdefault("LOG_LEVEL", "INFO")
    app.config.setdefault("LOG_RETENTION_DAYS", 7)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file_path = os.path.join(LOG_DIR, "app.log")

    log_level = getattr(
        logging,
        str(app.config.get("LOG_LEVEL", "INFO")).upper(),
        logging.INFO,
    )
    retention_days = int(app.config.get("LOG_RETENTION_DAYS", 7))

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Nettoyer les anciens handlers pour éviter les doublons
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    formatter = logging.Formatter("%(asctime)s - %(levelname)-8s - %(name)s - %(message)s")

    file_handler = TimedRotatingFileHandler(
        log_file_path,
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
        utc=False,
    )
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)
    stream_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)

    # Réduire le bruit des librairies externes
    for name in ["urllib3", "requests", "plexapi",
                 "urllib3.connectionpool", "requests.packages.urllib3",
                 "charset_normalizer"]:
        ext_logger = logging.getLogger(name)
        ext_logger.setLevel(logging.CRITICAL)
        ext_logger.propagate = False

    # ------------------------------------------------------------------
    # Context processor
    # ------------------------------------------------------------------
    @app.context_processor
    def inject_translator():
        def t(key, **kwargs):
            lang = app.config.get("LANGUAGE", "fr")
            return translate(lang, key, **kwargs)

        return {"t": t}

    # ------------------------------------------------------------------
    # Filtres / helpers Jinja
    # ------------------------------------------------------------------
    register_filters(app)

    @app.template_filter("b64encode")
    def b64encode_filter(value):
        if not value:
            return ""
        return base64.b64encode(value.encode("utf-8")).decode("utf-8")

    @app.template_filter("underscore_to_dash")
    def underscore_to_dash(value):
        try:
            s = str(value)
        except Exception:
            return value
        return re.sub(r"_(?=.)", " - ", s)

    def get_resolution_badge(res):
        res = str(res or "").lower()
        if "4k" in res:
            return '<span class="badge uhd">4K</span>'
        if "1080" in res:
            return '<span class="badge fullhd">1080p</span>'
        if "720" in res:
            return '<span class="badge hd">720p</span>'
        if "480" in res or "sd" in res:
            return '<span class="badge sd">SD</span>'
        return ""

    def get_codec_badge(codec):
        if not codec:
            return '<span class="badge codec badge-unknown">N/A</span>'
        lower = str(codec).lower()
        mapping = {
            "h264": "h264",
            "avc": "h264",
            "h265": "h265",
            "hevc": "hevc",
            "av1": "av1",
            "vp9": "vp9",
            "mpeg4": "mpeg4",
            "xvid": "xvid",
            "divx": "divx",
        }
        class_suffix = mapping.get(lower, "unknown")
        return f'<span class="badge codec badge-{class_suffix}">{str(codec).upper()}</span>'

    def get_bitrate_badge(bitrate):
        if not bitrate or not isinstance(bitrate, (int, float)) or bitrate == 0:
            return '<span class="badge bitrate bitrate-unk">N/A</span>'
        mbps = bitrate / 1000
        if mbps >= 10:
            level = "bitrate-high"
        elif mbps >= 5:
            level = "bitrate-med"
        else:
            level = "bitrate-low"
        return f'<span class="badge bitrate {level}">{mbps:.1f} Mbps</span>'

    app.jinja_env.globals.update(getResolutionBadge=get_resolution_badge)
    app.jinja_env.globals.update(getCodecBadge=get_codec_badge)
    app.jinja_env.globals.update(getBitrateBadge=get_bitrate_badge)

    # ------------------------------------------------------------------
    # Blueprints
    # ------------------------------------------------------------------
    from .routes import auth_bp, config_bp, dashboard_bp, my_dashboard_bp, mkv_bp, snapshot_bp, playlist_bp
    from .routes.mkv_routes import setup_auto_mkv_hook

    app.register_blueprint(auth_bp)
    app.register_blueprint(config_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(my_dashboard_bp)
    app.register_blueprint(mkv_bp)
    app.register_blueprint(ftp_bp)
    app.register_blueprint(snapshot_bp)
    app.register_blueprint(playlist_bp)

    # ------------------------------------------------------------------
    # Error handlers
    # ------------------------------------------------------------------
    @app.errorhandler(404)
    def not_found(e):
        if not session.get("logged_in"):
            from flask import redirect, url_for
            return redirect(url_for("auth.login"))
        return render_template("error.html",
            code=404,
            icon="🔍",
            message="Page introuvable.",
            detail=str(e)
        ), 404

    @app.errorhandler(403)
    def forbidden(e):
        return render_template("error.html",
            code=403,
            icon="🚫",
            message="Accès refusé.",
            detail=str(e)
        ), 403

    @app.errorhandler(500)
    def internal_error(e):
        app.logger.exception("Erreur 500")
        return render_template("error.html",
            code=500,
            icon="💥",
            message="Une erreur interne s'est produite.",
            detail=str(e)
        ), 500

    # ------------------------------------------------------------------
    # FTP queue worker
    # ------------------------------------------------------------------
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        with app.app_context():
            start_ftp_queue_worker(app)
            setup_auto_mkv_hook(app)

    return app