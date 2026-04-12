import json
import os
from copy import deepcopy

from app.config_paths import CONFIG_FILE

DEFAULT_CONFIG = {
    "FTP_SERVERS": [],          # Liste des serveurs FTP configurés
    "TMDB_API_KEY": "",
    "TRAKT_CLIENT_ID": "",
    "REFRESH_DELAY_DAYS": 30,
    "REFRESH_TIME": "03:00",

    "RESOLUTION_FILTER_MODE": "none",
    "COMPARE_USE_BITRATE": False,
    "MIN_BITRATE_DIFF_PERCENT": 10,
    "IGNORE_TRANSCODED_IN_BETTER": False,

    "SHOW_ONLY_LABELS": [],
    "EXCLUDED_LIBRARIES": [],

    "MKVTOOLNIX_BIN": "/usr/bin",
    "DEFAULT_AUDIO_LANG": "fr-CA",
    "DEFAULT_SUBTITLE_LANG": "fr-CA",

    "MKV_SOURCE_DIRS": [],
    "DESTINATIONS_MKV": {},

    "LOG_LEVEL": "INFO",
    "LANGUAGE": "fr",

    "FTP_ENABLED": False,
    "FTP_HOST": "",
    "FTP_PORT": 21,
    "FTP_USER": "",
    "FTP_PASS": "",
    "FTP_TLS": True,
    "FTP_PASSIVE": True,
    "FTP_ROOTS": [],
    "DOWNLOAD_PATH": "/dest",
    "SOURCE_PATH": "/sources",
    "FTP_DOWNLOAD_DIR_MOVIES": "/Inbox/FTP",
    "FTP_DOWNLOAD_DIR_SHOWS": "/Inbox/FTP",
    "FTP_INDEX_FILE": "",
    "FTP_REFRESH_HOURS": 12,

    "AUTO_DOWNLOAD_ENABLED": False,
    "AUTO_DOWNLOAD_START": "00:00",
    "AUTO_DOWNLOAD_END": "06:00",
    "FTP_AUTO_MAX_PER_DAY": 0,
    "FTP_AUTO_SKIP_DAYS": [],
    "MAX_PARALLEL_DOWNLOADS": 1,
    "AUTO_MKV_ENABLED": False,
    "AUTO_MKV_DST_MOVIES": "",
    "AUTO_MKV_DST_SHOWS": "",
    "LOG_RETENTION_DAYS": 7,

    "FTP_TITLE_ALIASES": {},
}


def get_config_path() -> str:
    return CONFIG_FILE


def _safe_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"1", "true", "yes", "on"}:
            return True
        if v in {"0", "false", "no", "off"}:
            return False
    return default


def _normalize_ftp_server(raw: dict, idx: int) -> dict:
    """Normalise un serveur FTP individuel."""
    return {
        "id":                 str(raw.get("id")   or f"ftp_{idx + 1}"),
        "name":               str(raw.get("name") or f"FTP {idx + 1}"),
        "enabled":            _safe_bool(raw.get("enabled"), True),
        "host":               str(raw.get("host")   or ""),
        "port":               _safe_int(raw.get("port"), 21),
        "user":               str(raw.get("user")   or ""),
        "pass":               str(raw.get("pass")   or ""),
        "tls":                _safe_bool(raw.get("tls"), False),
        "passive":            _safe_bool(raw.get("passive"), True),
        "roots":              raw.get("roots")              if isinstance(raw.get("roots"), list)        else [],
        "plex_servers":       raw.get("plex_servers")       if isinstance(raw.get("plex_servers"), list) else [],
        "download_dir_movies": str(raw.get("download_dir_movies") or "/Inbox/FTP"),
        "download_dir_shows":  str(raw.get("download_dir_shows")  or "/Inbox/FTP"),
        "refresh_hours":      _safe_int(raw.get("refresh_hours"), 12),
    }


def normalize_config(raw: dict | None) -> dict:
    cfg = deepcopy(DEFAULT_CONFIG)
    raw = raw or {}

    cfg["TMDB_API_KEY"]      = str(raw.get("TMDB_API_KEY",      cfg["TMDB_API_KEY"])      or "")
    cfg["TRAKT_CLIENT_ID"]   = str(raw.get("TRAKT_CLIENT_ID",   cfg["TRAKT_CLIENT_ID"])   or "")
    cfg["REFRESH_DELAY_DAYS"] = _safe_int(raw.get("REFRESH_DELAY_DAYS"), cfg["REFRESH_DELAY_DAYS"])
    cfg["REFRESH_TIME"] = str(raw.get("REFRESH_TIME", cfg["REFRESH_TIME"]) or "03:00")
    cfg["LOG_RETENTION_DAYS"] = _safe_int(raw.get("LOG_RETENTION_DAYS"), 7)

    mode = str(raw.get("RESOLUTION_FILTER_MODE", cfg["RESOLUTION_FILTER_MODE"]) or "none")
    if mode not in {"none", "equal_or_higher", "higher_only"}:
        mode = "none"
    cfg["RESOLUTION_FILTER_MODE"] = mode

    cfg["COMPARE_USE_BITRATE"] = _safe_bool(raw.get("COMPARE_USE_BITRATE"), cfg["COMPARE_USE_BITRATE"])
    cfg["MIN_BITRATE_DIFF_PERCENT"] = _safe_int(
        raw.get("MIN_BITRATE_DIFF_PERCENT"),
        cfg["MIN_BITRATE_DIFF_PERCENT"],
    )
    cfg["IGNORE_TRANSCODED_IN_BETTER"] = _safe_bool(
        raw.get("IGNORE_TRANSCODED_IN_BETTER"),
        cfg["IGNORE_TRANSCODED_IN_BETTER"],
    )

    excluded = raw.get("EXCLUDED_LIBRARIES", [])
    if isinstance(excluded, str):
        excluded = [x.strip() for x in excluded.split(",") if x.strip()]
    elif not isinstance(excluded, list):
        excluded = []
    cfg["EXCLUDED_LIBRARIES"] = excluded

    labels = raw.get("SHOW_ONLY_LABELS", [])
    if isinstance(labels, str):
        labels = [x.strip() for x in labels.split(",") if x.strip()]
    elif not isinstance(labels, list):
        labels = []
    cfg["SHOW_ONLY_LABELS"] = labels

    cfg["MKVTOOLNIX_BIN"] = str(raw.get("MKVTOOLNIX_BIN", cfg["MKVTOOLNIX_BIN"]) or "")
    cfg["DEFAULT_AUDIO_LANG"] = str(raw.get("DEFAULT_AUDIO_LANG", cfg["DEFAULT_AUDIO_LANG"]) or "fr-CA")
    _dsl = raw.get("DEFAULT_SUBTITLE_LANG", cfg["DEFAULT_SUBTITLE_LANG"])
    cfg["DEFAULT_SUBTITLE_LANG"] = str(_dsl) if _dsl is not None else "fr-CA"

    src_dirs = raw.get("MKV_SOURCE_DIRS", [])
    cfg["MKV_SOURCE_DIRS"] = src_dirs if isinstance(src_dirs, list) else []

    destinations = raw.get("DESTINATIONS_MKV", {})
    cfg["DESTINATIONS_MKV"] = destinations if isinstance(destinations, dict) else {}

    cfg["LOG_LEVEL"] = str(raw.get("LOG_LEVEL", cfg["LOG_LEVEL"]) or "INFO").upper()
    cfg["LANGUAGE"] = str(raw.get("LANGUAGE", cfg["LANGUAGE"]) or "fr").lower()

    # ── FTP_SERVERS — liste normalisée ──────────────────────────────────────
    raw_servers = raw.get("FTP_SERVERS")
    if isinstance(raw_servers, list) and raw_servers:
        # Config nouvelle structure : normaliser chaque serveur
        cfg["FTP_SERVERS"] = [_normalize_ftp_server(s, i) for i, s in enumerate(raw_servers)]
    elif raw.get("FTP_HOST"):
        # Migration depuis les clés plates (ancienne config) → FTP_SERVERS[0]
        cfg["FTP_SERVERS"] = [_normalize_ftp_server({
            "id":                  "ftp_1",
            "name":                "FTP principal",
            "enabled":             _safe_bool(raw.get("FTP_ENABLED"), False),
            "host":                str(raw.get("FTP_HOST") or ""),
            "port":                _safe_int(raw.get("FTP_PORT"), 21),
            "user":                str(raw.get("FTP_USER") or ""),
            "pass":                str(raw.get("FTP_PASS") or ""),
            "tls":                 _safe_bool(raw.get("FTP_TLS"), False),
            "passive":             _safe_bool(raw.get("FTP_PASSIVE"), True),
            "roots":               raw.get("FTP_ROOTS") if isinstance(raw.get("FTP_ROOTS"), list) else [],
            "plex_servers":        [],
            "download_dir_movies": str(raw.get("FTP_DOWNLOAD_DIR_MOVIES") or "/Inbox/FTP"),
            "download_dir_shows":  str(raw.get("FTP_DOWNLOAD_DIR_SHOWS")  or "/Inbox/FTP"),
            "refresh_hours":       _safe_int(raw.get("FTP_REFRESH_HOURS"), 12),
        }, 0)]
    else:
        cfg["FTP_SERVERS"] = []

    # Alias plats vers FTP_SERVERS[0] — rétrocompatibilité (ftp_download_service, etc.)
    _s0 = cfg["FTP_SERVERS"][0] if cfg["FTP_SERVERS"] else {}
    cfg["FTP_ENABLED"]            = _s0.get("enabled", False)
    cfg["FTP_HOST"]               = _s0.get("host", "")
    cfg["FTP_PORT"]               = _s0.get("port", 21)
    cfg["FTP_USER"]               = _s0.get("user", "")
    cfg["FTP_PASS"]               = _s0.get("pass", "")
    cfg["FTP_TLS"]                = _s0.get("tls", False)
    cfg["FTP_PASSIVE"]            = _s0.get("passive", True)
    cfg["FTP_ROOTS"]              = _s0.get("roots", [])
    cfg["FTP_DOWNLOAD_DIR_MOVIES"] = _s0.get("download_dir_movies", "/Inbox/FTP")
    cfg["FTP_DOWNLOAD_DIR_SHOWS"]  = _s0.get("download_dir_shows",  "/Inbox/FTP")
    cfg["FTP_REFRESH_HOURS"]      = _s0.get("refresh_hours", 12)
    cfg["FTP_INDEX_FILE"]         = ""

    cfg["DOWNLOAD_PATH"] = str(raw.get("DOWNLOAD_PATH", cfg["DOWNLOAD_PATH"]) or "/dest")
    cfg["SOURCE_PATH"] = str(raw.get("SOURCE_PATH", cfg["SOURCE_PATH"]) or "/sources")

    cfg["AUTO_DOWNLOAD_ENABLED"] = _safe_bool(
        raw.get("AUTO_DOWNLOAD_ENABLED"),
        cfg["AUTO_DOWNLOAD_ENABLED"],
    )
    cfg["AUTO_DOWNLOAD_START"] = str(
        raw.get("AUTO_DOWNLOAD_START", cfg["AUTO_DOWNLOAD_START"]) or "00:00"
    )
    cfg["AUTO_DOWNLOAD_END"] = str(
        raw.get("AUTO_DOWNLOAD_END", cfg["AUTO_DOWNLOAD_END"]) or "06:00"
    )
    cfg["FTP_AUTO_MAX_PER_DAY"] = _safe_int(raw.get("FTP_AUTO_MAX_PER_DAY"), 0)
    skip_days = raw.get("FTP_AUTO_SKIP_DAYS", [])
    if isinstance(skip_days, str):
        skip_days = [int(d.strip()) for d in skip_days.split(",") if d.strip().isdigit()]
    elif not isinstance(skip_days, list):
        skip_days = []
    cfg["FTP_AUTO_SKIP_DAYS"] = [int(d) for d in skip_days if str(d).isdigit() or isinstance(d, int)]
    cfg["AUTO_MKV_ENABLED"] = _safe_bool(raw.get("AUTO_MKV_ENABLED"), False)
    cfg["AUTO_MKV_DST_MOVIES"] = str(raw.get("AUTO_MKV_DST_MOVIES", "") or "")
    cfg["AUTO_MKV_DST_SHOWS"] = str(raw.get("AUTO_MKV_DST_SHOWS", "") or "")
    cfg["MAX_PARALLEL_DOWNLOADS"] = _safe_int(
        raw.get("MAX_PARALLEL_DOWNLOADS"),
        cfg["MAX_PARALLEL_DOWNLOADS"],
    )

    aliases = raw.get("FTP_TITLE_ALIASES", {})
    cfg["FTP_TITLE_ALIASES"] = aliases if isinstance(aliases, dict) else {}

    return cfg


def load_config() -> dict:
    path = get_config_path()

    if not os.path.exists(path) or os.path.getsize(path) == 0:
        cfg = normalize_config({})
        save_config(cfg)
        return cfg

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return normalize_config(raw)
    except Exception:
        cfg = normalize_config({})
        save_config(cfg)
        return cfg


def save_config(cfg: dict) -> dict:
    path = get_config_path()
    normalized = normalize_config(cfg)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(normalized, f, indent=2, ensure_ascii=False)

    return normalized


def build_config_from_form(form) -> dict:
    import json as _json
    dest_keys = form.getlist("dest_keys[]")
    dest_values = form.getlist("dest_values[]")

    _ftp_servers = None
    _ftp_servers_json = form.get("ftp_servers_json", "")
    if _ftp_servers_json:
        try:
            _ftp_servers = _json.loads(_ftp_servers_json)
        except Exception:
            _ftp_servers = []

    return normalize_config({
        "TMDB_API_KEY":    form.get("tmdb_api_key", ""),
        "TRAKT_CLIENT_ID": form.get("trakt_client_id", ""),
        "REFRESH_DELAY_DAYS": form.get("refresh_delay_days", 30),
        "REFRESH_TIME": form.get("refresh_time", "03:00"),
        "LOG_RETENTION_DAYS": form.get("log_retention_days", 7),

        "RESOLUTION_FILTER_MODE": form.get("resolution_filter_mode", "none"),
        "COMPARE_USE_BITRATE": "compare_use_bitrate" in form,
        "MIN_BITRATE_DIFF_PERCENT": form.get("min_bitrate_diff_percent", 10),
        "IGNORE_TRANSCODED_IN_BETTER": "ignore_transcoded_in_better" in form,

        "SHOW_ONLY_LABELS": [
            l.strip() for l in form.get("show_only_labels", "").split(",") if l.strip()
        ],

        "EXCLUDED_LIBRARIES": [
            l.strip() for l in form.get("excluded_libraries", "").split(",") if l.strip()
        ],

        "MKVTOOLNIX_BIN": form.get("mkvtoolnix_bin", ""),
        "DEFAULT_AUDIO_LANG": form.get("default_audio_lang", "fr-CA"),
        "DEFAULT_SUBTITLE_LANG": form.get("default_subtitle_lang", "fr-CA"),

        "MKV_SOURCE_DIRS": [
            v for v in form.getlist("mkv_source_dirs[]") if v.strip()
        ],

        "DESTINATIONS_MKV": {
            k: v for k, v in zip(dest_keys, dest_values) if k.strip() or v.strip()
        },

        "LOG_LEVEL": form.get("log_level", "INFO"),
        "LANGUAGE": form.get("language", "fr"),

        # Multi-FTP: if ftp_servers_json is present it takes priority over flat keys
        "FTP_SERVERS": _ftp_servers,

        # Flat FTP keys kept for backward compat (used when ftp_servers_json is absent)
        "FTP_ENABLED": "ftp_enabled" in form,
        "FTP_HOST": form.get("ftp_host", "").strip(),
        "FTP_PORT": form.get("ftp_port", 21),
        "FTP_USER": form.get("ftp_user", "").strip(),
        "FTP_PASS": form.get("ftp_pass", "").strip(),
        "FTP_TLS": "ftp_tls" in form,
        "FTP_PASSIVE": "ftp_passive" in form,

        "FTP_ROOTS": [
            v for v in form.getlist("ftp_base_dirs[]") if v.strip()
        ],

        "FTP_DOWNLOAD_DIR_MOVIES": form.get("ftp_download_dir_movies", "").strip(),
        "FTP_DOWNLOAD_DIR_SHOWS": form.get("ftp_download_dir_shows", "").strip(),
        "FTP_INDEX_FILE": "",
        "FTP_REFRESH_HOURS": 12,

        "AUTO_DOWNLOAD_ENABLED": "auto_download_enabled" in form,
        "AUTO_DOWNLOAD_START": form.get("auto_download_start", "00:00"),
        "AUTO_DOWNLOAD_END": form.get("auto_download_end", "06:00"),
        "FTP_AUTO_MAX_PER_DAY": form.get("ftp_auto_max_per_day", 0),
        "FTP_AUTO_SKIP_DAYS": [
            int(d.strip()) for d in form.get("ftp_auto_skip_days", "").split(",")
            if d.strip().isdigit()
        ],
        "AUTO_MKV_ENABLED": "auto_mkv_enabled" in form,
        "AUTO_MKV_DST_MOVIES": form.get("auto_mkv_dst_movies", "").strip(),
        "AUTO_MKV_DST_SHOWS": form.get("auto_mkv_dst_shows", "").strip(),

        "FTP_TITLE_ALIASES": {
            k.strip(): v.strip()
            for k, v in zip(
                form.getlist("alias_keys[]"),
                form.getlist("alias_values[]")
            )
            if k.strip() and v.strip()
        },
    })