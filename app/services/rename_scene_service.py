"""
rename_scene_service.py — Adaptation web de rename_scene.py pour plex-compare.
Toute la logique de renommage tourne dans un thread de fond avec suivi SSE.
"""

import re
import json
import time
import shutil
import threading
import subprocess
import urllib.request
import urllib.parse
from pathlib import Path
from types import SimpleNamespace

import logging

logger = logging.getLogger(__name__)

# ─── MediaInfo ────────────────────────────────────────────────────────────────
try:
    from pymediainfo import MediaInfo as _MediaInfo
    MEDIAINFO_AVAILABLE = True
except ImportError:
    MEDIAINFO_AVAILABLE = False

# ─── ffmpeg (validation d'intégrité) ───────────────────────────────────────────
FFMPEG_AVAILABLE = shutil.which("ffmpeg") is not None

# ─── Thread-local log collector ───────────────────────────────────────────────
_tl = threading.local()

def _p(msg):
    """Print-replacement: appends to job log when inside a worker thread."""
    fn = getattr(_tl, "log_fn", None)
    if fn is not None:
        fn(str(msg))
    else:
        logger.debug(str(msg))


# ─── Job state ────────────────────────────────────────────────────────────────
_rename_lock = threading.Lock()
_rename_status = {
    "running": False,
    "done": False,
    "total": 0,
    "processed": 0,
    "renamed": 0,
    "already_ok": 0,
    "errors": 0,
    "corrupted": 0,
    "unidentified": 0,
    "log": [],
}


def get_rename_job_status():
    with _rename_lock:
        s = dict(_rename_status)
        s["log"] = list(_rename_status["log"])
        return s


def _update(**kw):
    with _rename_lock:
        _rename_status.update(kw)


def _rename_log(msg):
    with _rename_lock:
        _rename_status["log"].append(msg)
        if len(_rename_status["log"]) > 500:
            _rename_status["log"] = _rename_status["log"][-500:]


# ═══════════════════════════════════════════════════════════════════════════════
#  CONSTANTES (identiques à rename_scene.py)
# ═══════════════════════════════════════════════════════════════════════════════

TMDB_API_KEY   = ""   # injecté au lancement depuis app.config
TMDB_COUNTRY   = "CA"
JUSTWATCH_COUNTRY = "CA"

SERVICE_TAGS = {
    "netflix": "NF", "amazon": "AMZN", "prime video": "AMZN",
    "disney": "DSNP", "hulu": "HULU", "hbo": "MAX", " max ": "MAX",
    "apple": "ATVP", "appletv": "ATVP", "paramount": "PMTP",
    "peacock": "PCOK", "crunchyroll": "CR", "youtube": "YT",
    "crave": "CRAV", "toutv": "TOUTV", "tou.tv": "TOUTV",
    "illico": "ILLICO", "club illico": "CLUB", "stacktv": "STACKTV",
    "stack tv": "STACKTV", "cbc gem": "GEM", "gem": "GEM",
    "ctv": "CTV", "discovery": "DSCP", "sportsnet": "SNET",
    "tsn": "TSN", "tubi": "TUBI", "pluto": "PLUTO", "hotstar": "HTSR",
}

TMDB_PROVIDERS = {
    "Netflix": "NF", "Amazon Prime Video": "AMZN", "Amazon Video": "AMZN",
    "Disney Plus": "DSNP", "Disney+": "DSNP", "Apple TV Plus": "ATVP",
    "Apple TV+": "ATVP", "Apple TV": "ATVP", "Max": "MAX", "HBO Max": "MAX",
    "Hulu": "HULU", "Paramount Plus": "PMTP", "Paramount+": "PMTP",
    "Peacock": "PCOK", "Crave": "CRAV", "ICI Tou.tv": "TOUTV",
    "Tou.tv": "TOUTV", "Illico": "ILLICO", "Club illico": "CLUB",
    "StackTV": "STACKTV", "CBC Gem": "GEM", "Gem": "GEM", "CTV": "CTV",
    "Crunchyroll": "CR", "Tubi TV": "TUBI", "Pluto TV": "PLUTO",
    "Discovery Plus": "DSCP", "Sportsnet Now": "SNET", "TSN Direct": "TSN",
}

JUSTWATCH_PROVIDERS = {
    "NF": ["Netflix"], "AMZN": ["Amazon Prime Video"],
    "DSNP": ["Disney Plus", "Disney+"], "ATVP": ["Apple TV"],
    "MAX": ["Max", "HBO Max"], "HULU": ["Hulu"],
    "PMTP": ["Paramount Plus", "Paramount+"], "PCOK": ["Peacock"],
    "CR": ["Crunchyroll"], "DSCP": ["Discovery Plus", "Discovery+"],
    "PLUTO": ["Pluto TV"], "TUBI": ["Tubi"],
    "HTSR": ["Disney Plus Hotstar", "Hotstar"],
    "CRAV": ["Crave"], "TOUTV": ["ICI Tou.tv", "Tou.tv"],
    "ILLICO": ["Illico"], "CLUB": ["Club illico"],
    "STACKTV": ["StackTV", "Stack TV"], "GEM": ["CBC Gem", "Gem"],
    "CTV": ["CTV"], "SNET": ["Sportsnet Now", "Sportsnet"],
    "TSN": ["TSN Direct", "TSN"],
}

INFERENCE_RULES = [
    {"note": "AV1+DDP→NF", "match": {"video_codec": "AV1", "audio_codec": "DDP"}, "service": "NF", "source": "WEB-DL", "confidence": "certain"},
    {"note": "AV1+AAC2.0→YT", "match": {"video_codec": "AV1", "audio_codec": "AAC", "audio_channels": "2.0"}, "service": "YT", "source": "WEBRip", "confidence": "high"},
    {"note": "AV1+AAC→NF/YT", "match": {"video_codec": "AV1", "audio_codec": "AAC"}, "service": None, "candidates": ["NF", "YT"], "source": "WEB-DL", "confidence": "ambiguous"},
    {"note": "H265+HDR10+→ATVP", "match": {"video_codec": "H265", "hdr": "HDR10Plus"}, "service": "ATVP", "source": "WEB-DL", "confidence": "high"},
    {"note": "H265+DV→DSNP/ATVP", "match": {"video_codec": "H265", "hdr": "DV"}, "service": None, "candidates": ["DSNP", "ATVP"], "source": "WEB-DL", "confidence": "ambiguous"},
    {"note": "H265+HDR10+TrueHD→DSNP/ATVP", "match": {"video_codec": "H265", "hdr": "HDR10", "audio_codec": "TrueHD"}, "service": None, "candidates": ["DSNP", "ATVP"], "source": "WEB-DL", "confidence": "ambiguous"},
    {"note": "H265+HDR10→DSNP", "match": {"video_codec": "H265", "hdr": "HDR10"}, "service": "DSNP", "source": "WEB-DL", "confidence": "medium"},
    {"note": "H265+DDP→WEB-DL ambigu", "match": {"video_codec": "H265", "audio_codec": "DDP"}, "service": None, "candidates": ["AMZN", "DSNP", "ATVP", "MAX", "NF", "CRAV", "PMTP"], "source": "WEB-DL", "confidence": "ambiguous"},
    {"note": "H264+DDP→WEB-DL ambigu", "match": {"video_codec": "H264", "audio_codec": "DDP"}, "service": None, "candidates": ["NF", "AMZN", "DSNP", "ATVP", "MAX", "PMTP", "CRAV"], "source": "WEB-DL", "confidence": "ambiguous"},
    {"note": "VP9+AAC→YT", "match": {"video_codec": "VP9", "audio_codec": "AAC"}, "service": "YT", "source": "WEBRip", "confidence": "high"},
    {"note": "H265+AAC2.0→CR/YT", "match": {"video_codec": "H265", "audio_codec": "AAC", "audio_channels": "2.0"}, "service": None, "candidates": ["CR", "YT"], "source": "WEBRip", "confidence": "ambiguous"},
    {"note": "H264+AAC2.0→CR/gratuit", "match": {"video_codec": "H264", "audio_codec": "AAC", "audio_channels": "2.0"}, "service": None, "candidates": ["CR", "TUBI", "GEM", "CTV"], "source": "WEBRip", "confidence": "ambiguous"},
    {"note": "H265+AAC5.1→WEB-DL ambigu", "match": {"video_codec": "H265", "audio_codec": "AAC", "audio_channels": "5.1"}, "service": None, "candidates": ["NF", "AMZN", "DSNP", "ATVP", "CRAV", "PMTP"], "source": "WEB-DL", "confidence": "ambiguous"},
    {"note": "H264+AAC5.1→WEB-DL ambigu", "match": {"video_codec": "H264", "audio_codec": "AAC", "audio_channels": "5.1"}, "service": None, "candidates": ["NF", "AMZN", "DSNP", "ATVP", "CRAV", "PMTP"], "source": "WEB-DL", "confidence": "ambiguous"},
    {"note": "MPEG2→DVDRip", "match": {"video_codec": "MPEG2"}, "service": None, "source": "DVDRip", "confidence": "medium"},
    {"note": "H264+DD→WEBRip", "match": {"video_codec": "H264", "audio_codec": "DD"}, "service": None, "source": "WEBRip", "confidence": "medium"},
    {"note": "H265+DD→WEBRip", "match": {"video_codec": "H265", "audio_codec": "DD"}, "service": None, "source": "WEBRip", "confidence": "medium"},
    {"note": "H264+DTS→WEB-DL ambigu", "match": {"video_codec": "H264", "audio_codec": "DTS"}, "service": None, "candidates": ["NF", "AMZN", "DSNP", "ATVP", "MAX", "CRAV", "PMTP"], "source": "WEB-DL", "confidence": "ambiguous"},
    {"note": "H265+DTS→WEB-DL ambigu", "match": {"video_codec": "H265", "audio_codec": "DTS"}, "service": None, "candidates": ["NF", "AMZN", "DSNP", "ATVP", "MAX", "CRAV", "PMTP"], "source": "WEB-DL", "confidence": "ambiguous"},
    {"note": "XviD→DVDRip", "match": {"video_codec": "XviD"}, "service": None, "source": "DVDRip", "confidence": "high"},
    {"note": "WMV→WEBRip", "match": {"video_codec": "WMV"}, "service": None, "source": "WEBRip", "confidence": "medium"},
]

SCENE_TAG_PATTERN = re.compile(r'-([A-Za-z][A-Za-z0-9]{1,12})$')
SCENE_TAG_EXCLUSIONS = {
    "264", "265", "H264", "H265", "AV1", "VP9", "VC1",
    "DDP", "AAC", "DTS", "AC3", "DD", "EAC3",
    "HDR", "SDR", "DV", "HLG", "HDR10",
    "MKV", "MP4", "AVI", "MOV",
    "WEB", "HDTV", "BluRay", "BDRip",
}
VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts", ".webm", ".wmv", ".flv", ".mpg", ".mpeg"}

ACCENT_MAP = str.maketrans(
    "àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ"
    "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ",
    "aaaaaaeceeeeiiiidnoooooouuuuytÿ"
    "AAAAAAECEEEEIIIIÐNOOOOOOUUUUYÞ"
)

EDITION_KEYWORDS = [
    # Handle "Director's Cut", "Directors Cut", "Director s Cut" (apostrophe stripped → space)
    (r"ultimate\s+director\s*'?\s*s?\s+cut", "Ultimate Director's Cut"),
    (r"unrated\s+director\s*'?\s*s?\s+cut", "Unrated Director's Cut"),
    (r"director\s*'?\s*s?\s+cut", "Director's Cut"),
    (r"extended\s+(cut|edition|version|remaster)", "Extended Edition"),
    (r"theatrical\s+(cut|edition|version)", "Theatrical Edition"),
    (r"special\s+edition", "Special Edition"),
    (r"ultimate\s+edition", "Ultimate Edition"),
    (r"anniversary\s+edition", "Anniversary Edition"),
    (r"final\s+cut", "Final Cut"),
    (r"fan\s+edit", "Fan Edit"),
    (r"international\s+(cut|version)", "International Cut"),
    (r"japanese\s+(cut|version)", "Japanese Cut"),
    (r"extended", "Extended Edition"),
    (r"theatrical", "Theatrical Edition"),
    (r"unrated", "Unrated"),
    (r"uncensored", "Uncensored"),
    (r"remastered", "Remastered"),
    (r"redux", "Redux"),
    (r"uncut", "Uncut"),
    (r"imax", "IMAX"),
]

GENERIC_EPISODE_TITLES = re.compile(
    r"^(episode|ep|épisode|chapitre|chapter|partie|part|saison|season|pilote|pilot)"
    r"[\s\.\-]*\d*$|^\d+$", re.IGNORECASE
)

SEASON_DIR_PATTERN = re.compile(r'^(season|saison)\s*(\d{1,2})$|^[Ss](\d{1,2})$', re.IGNORECASE)
SERIES_YEAR_PATTERN = re.compile(r'\((\d{4})\)\s*$')

KNOWN_SERVICE_TAGS = {
    "NF", "AMZN", "DSNP", "ATVP", "MAX", "HULU", "PMTP", "PCOK",
    "CRAV", "TOUTV", "CR", "TUBI", "PLUTO", "GEM", "CTV", "STACKTV",
    "WEB-DL", "WEBDL", "WEBRIP", "BLURAY", "REMUX", "DVDRIP", "BDRIP",
}

LANG_TAGS = {
    "fr": "FRENCH", "fre": "FRENCH", "fra": "FRENCH",
    "fr-fr": "VFF", "fr-ca": "VFQ", "fr-be": "FRENCH",
    "en": "ENGLISH", "eng": "ENGLISH",
    "es": "SPANISH", "spa": "SPANISH",
    "de": "GERMAN", "deu": "GERMAN", "ger": "GERMAN",
    "it": "ITALIAN", "ita": "ITALIAN",
    "pt": "PORTUGUESE", "por": "PORTUGUESE",
    "ja": "JAPANESE", "jpn": "JAPANESE",
    "ko": "KOREAN", "kor": "KOREAN",
    "zh": "CHINESE", "zho": "CHINESE", "chi": "CHINESE",
    "ar": "ARABIC", "ara": "ARABIC",
    "ru": "RUSSIAN", "rus": "RUSSIAN",
    "nl": "DUTCH", "nld": "DUTCH",
    "pl": "POLISH", "pol": "POLISH",
}

TECHNICAL_TAGS_PATTERN = re.compile(
    r'''\b(2160p|1080p|720p|576p|480p|360p|NF|AMZN|DSNP|ATVP|MAX|HULU|PMTP|PCOK|CR|
    CRAV|TOUTV|GEM|CTV|STACKTV|YT|TUBI|PLUTO|WEB-DL|WEBRip|BluRay\.REMUX|DVD\.REMUX|
    DVDRip|BDRip|BluRay|HDTV|HDR10Plus|HDR10|DV|HLG|TrueHD\.Atmos|EAC3\.Atmos|
    DTS-HD\.MA|DTS-MA|DTS-X|EAC3|DDP|DD|DTS|AAC|FLAC|PCM|MP3|MP2|TrueHD|OPUS|
    7\.1|5\.1|2\.0|1\.0|H264|H265|AV1|VP9|MPEG2|XviD|WMV|MULTi|FRENCH|VFF|VFQ|
    VOSTFR|VOSEN|ENGLISH|JAPANESE|REMUX|REMASTERED|PROPER)\b''',
    re.IGNORECASE | re.VERBOSE
)


# ═══════════════════════════════════════════════════════════════════════════════
#  CACHES (réinitialisés à chaque job)
# ═══════════════════════════════════════════════════════════════════════════════

_tmdb_cache = {}
_tmdb_full_cache = {}
_tmdb_year_cache = {}
_jw_cache = {}
_ep_title_cache = {}
_series_year_cache = {}
_forced_tmdb_id = None


def _reset_caches():
    global _tmdb_cache, _tmdb_full_cache, _tmdb_year_cache, _jw_cache, _ep_title_cache, _series_year_cache, _forced_tmdb_id
    _tmdb_cache = {}
    _tmdb_full_cache = {}
    _tmdb_year_cache = {}
    _jw_cache = {}
    _ep_title_cache = {}
    _series_year_cache = {}
    _forced_tmdb_id = None


# ═══════════════════════════════════════════════════════════════════════════════
#  FONCTIONS UTILITAIRES
# ═══════════════════════════════════════════════════════════════════════════════

def to_ascii(text, keep_apostrophe=False, keep_accents=False):
    # Normalize typographic apostrophes (U+2018, U+2019) to straight apostrophe before encoding
    result = text.replace('\u2018', "'").replace('\u2019', "'")
    if not keep_accents:
        result = result.translate(ACCENT_MAP)
        result = result.encode("ascii", "ignore").decode("ascii")
    result = result.replace('"', "").replace("`", "")
    if not keep_apostrophe:
        result = result.replace("'", "")
    return result


def strip_edition(stem):
    """
    Detect and remove edition text from stem.
    Returns (clean_stem, edition_label) or (stem, None).
    Handles {edition-...}, [...], and plain-text keywords like "Director s Cut".
    """
    # Explicit Plex {edition-...} marker
    m0 = re.search(r'\{edition-([^}]+)\}', stem)
    if m0:
        label = m0.group(1).strip()
        clean = re.sub(r'\s+', ' ', (stem[:m0.start()] + ' ' + stem[m0.end():]).strip())
        return clean, label

    # Bracketed [Edition Name]
    m = re.search(r'\[([^\]]+)\]', stem)
    if m:
        label = m.group(1).strip()
        if not re.match(r'^(19|20)\d{2}$', label) and not re.match(r'^\d{3,4}p$', label):
            clean = re.sub(r'\s+', ' ', (stem[:m.start()] + ' ' + stem[m.end():]).strip())
            return clean, label

    # Keyword-based (plain text in stem)
    stem_lower = stem.lower()
    for pattern, label in EDITION_KEYWORDS:
        m = re.search(pattern, stem_lower, re.IGNORECASE)
        if m:
            clean = re.sub(r'\s+', ' ', (stem[:m.start()] + ' ' + stem[m.end():]).strip())
            return clean, label

    return stem, None


def detect_edition(stem):
    """Returns the edition label only (backward-compat wrapper around strip_edition)."""
    _, label = strip_edition(stem)
    return label


def get_media_info(filepath):
    if not MEDIAINFO_AVAILABLE:
        return None
    try:
        mi = _MediaInfo.parse(filepath)
    except Exception as e:
        _p(f"  ⚠️  Impossible d'analyser {Path(filepath).name} : {e}")
        return None

    info = {
        "resolution": None, "video_codec": None,
        "audio_codec": None, "audio_channels": None,
        "hdr": None, "writing_app": None, "encoded_by": None,
        "meta_title": None, "subtitle_format": None,
        "audio_tracks": [], "sub_tracks": [], "encoded_date": None,
    }

    for track in mi.tracks:
        if track.track_type == "General":
            info["writing_app"] = track.writing_application or track.writing_library or ""
            info["encoded_by"]  = track.encoded_by or track.comment or ""
            info["meta_title"]  = track.title or track.movie_name or ""
            info["encoded_date"] = track.encoded_date or track.tagged_date or ""

        elif track.track_type == "Video" and info["video_codec"] is None:
            h = track.height or 0
            w = track.width  or 0
            # Certains masters UHD (ex. Apple TV+) sont livrés en 2.00:1 (3840x1920)
            # plutôt qu'en 16:9 (3840x2160) : se fier à la seule hauteur les ferait
            # passer pour du 1080p alors que la largeur est bien de la 4K.
            if   h >= 2160 or w >= 3200: info["resolution"] = "2160p"
            elif h >= 1080: info["resolution"] = "1080p"
            elif h >= 720:  info["resolution"] = "720p"
            elif h >= 576:  info["resolution"] = "576p"
            elif h >= 480:  info["resolution"] = "480p"
            else:           info["resolution"] = f"{h}p"

            codec = (track.codec_id or track.format or "").upper()
            if   "AVC"  in codec or "H264" in codec: info["video_codec"] = "H264"
            elif "HEVC" in codec or "H265" in codec or "HEV1" in codec or "HVC1" in codec: info["video_codec"] = "H265"
            elif "AV1"  in codec or "AV01" in codec: info["video_codec"] = "AV1"
            elif "VP9"  in codec or "VP09" in codec: info["video_codec"] = "VP9"
            elif codec == "27":                       info["video_codec"] = "H264"
            elif codec == "33":                       info["video_codec"] = "H265"
            elif "VC1"  in codec:                    info["video_codec"] = "VC1"
            elif "MPEG" in codec:                    info["video_codec"] = "MPEG2"
            elif "XVID" in codec or "DIVX" in codec or "DX50" in codec or "DIV3" in codec:
                info["video_codec"] = "XviD"
            elif "V_MS" in codec or "WMV" in codec:  info["video_codec"] = "WMV"
            else: info["video_codec"] = codec.split("/")[0][:6] if codec else "UNK"

            hdr_f = (track.hdr_format or track.colour_primaries or "").lower()
            trans  = (track.transfer_characteristics or "").lower()
            if   "dolby vision" in hdr_f:                        info["hdr"] = "DV"
            elif "hdr10+" in hdr_f:                              info["hdr"] = "HDR10Plus"
            elif "hdr10" in hdr_f or "smpte st 2084" in trans:  info["hdr"] = "HDR10"
            elif "hlg" in hdr_f or "hlg" in trans:              info["hdr"] = "HLG"

        elif track.track_type == "Audio":
            codec = (track.codec_id or track.format or "").upper()
            ch    = track.channel_s or 0
            lang  = (track.language or track.other_language or "").lower().strip()

            if   ch >= 8: ch_tag = "7.1"
            elif ch >= 6: ch_tag = "5.1"
            elif ch == 2: ch_tag = "2.0"
            elif ch == 1: ch_tag = "1.0"
            else:         ch_tag = str(ch)

            if "EAC3" in codec or "E-AC-3" in codec or "A_EAC3" in codec or "EC-3" in codec:
                atmos = " ".join(filter(None, [
                    track.format_additionalfeatures or "",
                    getattr(track, "format_commercial_ifany", "") or "",
                ])).lower()
                a_codec = "EAC3.Atmos" if "atmos" in atmos else "EAC3"
            elif "TRUEHD" in codec or "A_TRUEHD" in codec:
                atmos = " ".join(filter(None, [
                    track.format_additionalfeatures or "",
                    getattr(track, "format_commercial_ifany", "") or "",
                ])).lower()
                a_codec = "TrueHD.Atmos" if "atmos" in atmos else "TrueHD"
            elif "AC-3" in codec or "AC3" in codec or "A_AC3" in codec: a_codec = "DD"
            elif "DTS" in codec:
                if "MA" in codec or "MASTER" in codec: a_codec = "DTS-MA"
                elif "X" in codec:                     a_codec = "DTS-X"
                else:                                  a_codec = "DTS"
            elif "AAC" in codec or "MP4A" in codec: a_codec = "AAC"
            elif "OPUS" in codec or "A_OPUS" in codec: a_codec = "OPUS"
            elif "FLAC" in codec: a_codec = "FLAC"
            elif "PCM"  in codec: a_codec = "PCM"
            elif "MP3"  in codec or "MPEG AUDIO" in codec: a_codec = "MP3"
            else: a_codec = codec[:6] if codec else "UNK"

            info["audio_tracks"].append({"lang": lang, "codec": a_codec, "channels": ch_tag})
            if info["audio_codec"] is None:
                info["audio_codec"]    = a_codec
                info["audio_channels"] = ch_tag

        elif track.track_type == "Text":
            fmt  = (track.format   or "").upper()
            cod  = (track.codec_id or "").upper()
            lang = (track.language or track.other_language or "").lower().strip()

            if info["subtitle_format"] is None:
                if   "VOBSUB" in fmt or "DVD" in fmt:  info["subtitle_format"] = "VobSub"
                elif "PGS" in fmt or "HDMV" in fmt:    info["subtitle_format"] = "PGS"
                elif "ASS" in fmt or "SSA" in fmt:     info["subtitle_format"] = "ASS"
                elif "TX3G" in cod or "SBTL" in cod:   info["subtitle_format"] = "sbtl"
                elif "SRT" in fmt or "UTF" in fmt:     info["subtitle_format"] = "SRT"
            if lang:
                info["sub_tracks"].append(lang)

    return info


def validate_video_integrity(filepath, timeout=1800):
    """
    Vérifie l'intégrité d'un fichier vidéo par décodage complet (ffmpeg).
    Contrairement à une simple lecture d'en-tête, ceci détecte la corruption
    en milieu de fichier (ex: téléchargement ou copie interrompue). Plus lent
    qu'une simple vérification d'en-tête mais seule méthode fiable.
    Retourne (is_valid, message).
    """
    cmd = [
        "ffmpeg", "-v", "error", "-xerror",
        "-i", str(filepath),
        "-f", "null", "-",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f"Timeout après {timeout}s"
    except Exception as e:
        return False, str(e)

    if result.returncode != 0:
        lines = [l for l in result.stderr.strip().splitlines() if l.strip()]
        return False, (lines[-1][:200] if lines else "erreur de décodage")

    return True, ""


def _dedupe_dest(dest_dir, filename):
    dest = dest_dir / filename
    if dest.exists():
        stem, suffix = Path(filename).stem, Path(filename).suffix
        counter = 1
        while dest.exists():
            dest = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1
    return dest


def _move_file(filepath, dest_dir, filename, dry_run):
    """Déplace filepath vers dest_dir/filename (dédupliqué). Retourne le Path final ou None si échec."""
    if dry_run:
        _p(f"  🔍 [DRY RUN] Déplacement simulé → {dest_dir / filename}")
        return dest_dir / filename

    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        _p(f"  ⚠️  Impossible de créer {dest_dir} : {e}")
        return None

    dest = _dedupe_dest(dest_dir, filename)
    try:
        filepath.rename(dest)
    except Exception:
        try:
            shutil.move(str(filepath), str(dest))
        except Exception as e:
            _p(f"  ⚠️  Impossible de déplacer vers {dest_dir} : {e}")
            return None
    return dest


def move_to_corrupted(filepath, corrupted_dir, dry_run):
    """Déplace un fichier corrompu vers le dossier de quarantaine (évite d'écraser)."""
    dest = _move_file(filepath, Path(corrupted_dir), filepath.name, dry_run)
    if dest and not dry_run:
        _p(f"  📦 Déplacé vers Corrompus : {dest.name}")
    return dest


def sort_output_file(filepath, new_name, opts, category, title=None, year=None, season_num=None):
    """
    Déplace filepath vers opts.output_dir/<Catégorie>/... selon le résultat du traitement.
    category : "renamed" | "unidentified" | "corrupted" | "error"
    """
    root = Path(opts.output_dir)
    if category == "renamed":
        folder_name = f"{title} ({year})" if (title and year) else (title or "Inconnu")
        dest_dir = root / "Renommés" / folder_name
        if season_num:
            dest_dir = dest_dir / f"Season {season_num:02d}"
    elif category == "unidentified":
        dest_dir = root / "Non_Identifiés"
    elif category == "corrupted":
        dest_dir = root / "Corrompus"
    else:
        dest_dir = root / "Non_Renommés"

    dest = _move_file(filepath, dest_dir, new_name, opts.dry_run)
    if dest is None and category == "renamed":
        # Chemin imbriqué en échec (dossier titre invalide, chemin trop long...) — repli à plat.
        dest = _move_file(filepath, root / "Non_Renommés", new_name, opts.dry_run)

    if dest and not opts.dry_run:
        _p(f"  📦 Déplacé → {dest}")
    return dest


def _fetch_tmdb_year(tmdb_id, media_type):
    cache_key = f"{media_type}|{tmdb_id}"
    if cache_key in _tmdb_year_cache:
        return _tmdb_year_cache[cache_key]
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        date = data.get("release_date") if media_type == "movie" else data.get("first_air_date")
        result = (date or "")[:4] or None
    except Exception:
        result = None
    _tmdb_year_cache[cache_key] = result
    return result


def check_identified(title, year, is_episode):
    """
    Confirme l'existence du titre sur TMDB (indépendant de la détection du service).
    Pour une série, l'année retournée est toujours celle de la première diffusion
    (constante sur toutes les saisons) — jamais l'année embarquée dans le nom du
    fichier de la saison en cours, qui varie d'une saison à l'autre et ferait
    éclater la série en plusieurs dossiers "Titre (Année)" distincts.
    """
    if not title:
        return False, None
    # Pour une série, ne jamais filtrer la recherche TMDB par l'année de la saison en
    # cours : "first_air_date_year" est un filtre strict côté API, et une année de
    # saison ultérieure (ex. saison 3 sortie en 2025) peut faire manquer ou mal
    # cibler la fiche TMDB de la série (dont la première diffusion est 2021).
    search_year = None if is_episode else year
    tmdb_id, media_type = tmdb_search(title, search_year, is_series=is_episode)
    if not tmdb_id:
        return False, None
    if is_episode:
        return True, _fetch_tmdb_year(tmdb_id, media_type) or year
    if year:
        return True, year
    return True, _fetch_tmdb_year(tmdb_id, media_type)


def detect_existing_scene_tag(stem):
    _TECH = r'(?:2160|1080|720|576|480|432|360)p|WEB-DL|WEBRip|BluRay|REMUX|DVDRip|BDRip|HDTV'
    if not re.search(_TECH, stem, re.IGNORECASE):
        return None
    m = SCENE_TAG_PATTERN.search(stem)
    if m:
        tag = m.group(1)
        if tag.upper() not in {e.upper() for e in SCENE_TAG_EXCLUSIONS}:
            return tag
    return None


def _best_service_match(haystack):
    """
    Retourne le tag du mot-clé SERVICE_TAGS le plus spécifique (le plus long) trouvé
    dans `haystack`, avec limites de mot — évite qu'une abréviation courte et générique
    (ex. "hbo") ne masque une correspondance plus précise (ex. "appletv") simplement
    parce qu'elle est testée plus tôt dans l'ordre du dictionnaire.
    """
    matches = []
    for keyword, tag in SERVICE_TAGS.items():
        kw = keyword.strip()
        if kw and re.search(r'(?<![a-z0-9])' + re.escape(kw) + r'(?![a-z0-9])', haystack):
            matches.append((kw, tag))
    if not matches:
        return None
    matches.sort(key=lambda kv: len(kv[0]), reverse=True)
    return matches[0][1]


def detect_service_from_name(filepath, media_info):
    filepath = Path(filepath)
    # Le nom de fichier est la source la plus fiable (tag posé explicitement par la
    # release) ; les dossiers parents et les métadonnées ne servent que de repli, pour
    # qu'un mot générique trouvé ailleurs (ex. un dossier de téléchargement) ne
    # l'emporte pas sur le vrai tag présent dans le nom du fichier.
    tag = _best_service_match(filepath.name.lower())
    if tag:
        return tag

    parents = " ".join(p.name.lower() for p in filepath.parents[:3])
    tag = _best_service_match(parents)
    if tag:
        return tag

    if media_info:
        meta = " ".join([
            (media_info.get("writing_app") or "").lower(),
            (media_info.get("encoded_by")  or "").lower(),
        ])
        tag = _best_service_match(meta)
        if tag:
            return tag

    return None


def detect_anystream_service(media_info):
    writing = (media_info.get("writing_app") or "").strip()
    subs    = media_info.get("subtitle_format", "")
    comment = (media_info.get("encoded_by") or "").lower()
    if writing or subs != "sbtl":
        return None, None
    if "disney" in comment: return "DSNP", "WEB-DL"
    if "amazon" in comment or "prime" in comment: return "AMZN", "WEB-DL"
    if "netflix" in comment: return "NF", "WEB-DL"
    if "apple" in comment: return "ATVP", "WEB-DL"
    if "hbo" in comment or " max" in comment: return "MAX", "WEB-DL"
    if "paramount" in comment: return "PMTP", "WEB-DL"
    if "crave" in comment: return "CRAV", "WEB-DL"
    return None, "WEB-DL"


def detect_source_from_app(media_info):
    writing = (media_info.get("writing_app") or "").lower()
    video   = media_info.get("video_codec", "")
    subs    = media_info.get("subtitle_format", "")
    res     = media_info.get("resolution", "")
    if "makemkv" in writing:
        if video == "MPEG2" or subs == "VobSub" or res in ("480p", "576p"):
            return None, "DVD.REMUX"
        return None, "BluRay.REMUX"
    if "handbrake" in writing:
        res_num = int(res.replace("p", "")) if res and res.endswith("p") else 9999
        if res_num <= 576 or subs == "VobSub": return None, "DVDRip"
        if res_num >= 1080: return None, "BDRip"
        return None, "DVDRip"
    return None, None


def detect_service_from_mkvmerge(media_info):
    writing = (media_info.get("writing_app") or "").lower() if media_info else ""
    if "mkvmerge" not in writing:
        return None, None
    video = (media_info.get("video_codec") or "") if media_info else ""
    audio = (media_info.get("audio_codec") or "") if media_info else ""
    ch    = (media_info.get("audio_channels") or "") if media_info else ""
    if video == "H264" and audio == "AAC" and ch == "2.0":
        return "CR", "WEBRip"
    return None, None


def infer_from_codec(media_info):
    for rule in INFERENCE_RULES:
        m = rule["match"]
        if all(media_info.get(k) is not None and media_info.get(k) == v for k, v in m.items()):
            return (rule.get("service"), rule.get("source"), rule["confidence"],
                    rule.get("candidates", []), rule["note"])
    return None, None, "none", [], ""


# ─── TMDB ─────────────────────────────────────────────────────────────────────

def tmdb_search(title, year, is_series=False):
    global _forced_tmdb_id
    if _forced_tmdb_id is not None:
        return _forced_tmdb_id, ("tv" if is_series else "movie")

    cache_key = f"{title}|{year}|{is_series}"
    if cache_key in _tmdb_cache:
        return _tmdb_cache[cache_key]

    media_type = "tv" if is_series else "movie"
    for lang in ["fr-CA", "fr-FR", "en-US"]:
        params = {"api_key": TMDB_API_KEY, "query": title, "language": lang, "include_adult": "false"}
        if year:
            params["year" if not is_series else "first_air_date_year"] = year
        url = f"https://api.themoviedb.org/3/search/{media_type}?" + urllib.parse.urlencode(params)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception:
            continue

        results = data.get("results") or []
        title_norm = re.sub(r'[^a-z0-9]', '', title.lower())

        def _year_ok(r):
            r_year = (r.get("release_date") or r.get("first_air_date") or "")[:4]
            if year and r_year:
                try:
                    return abs(int(r_year) - int(year)) <= 1
                except ValueError:
                    return True
            return True

        # Passe 1 : correspondance exacte (ignore spin-offs et suites)
        for r in results[:10]:
            r_norm = re.sub(r'[^a-z0-9]', '', (r.get("title") or r.get("name") or "").lower())
            if r_norm == title_norm and _year_ok(r):
                result = (r.get("id"), media_type)
                _tmdb_cache[cache_key] = result
                return result

        # Passe 2 : sous-chaîne, mais préférer le titre le plus proche en longueur
        # (évite de matcher "Slingshot" quand on cherche "Agents of SHIELD")
        candidates = []
        for r in results[:10]:
            r_title = r.get("title") or r.get("name") or ""
            r_norm  = re.sub(r'[^a-z0-9]', '', r_title.lower())
            if (title_norm in r_norm or r_norm in title_norm) and _year_ok(r):
                # Score = différence de longueur (plus petit = meilleur match)
                length_diff = abs(len(r_norm) - len(title_norm))
                popularity  = r.get("popularity", 0)
                candidates.append((length_diff, -popularity, r.get("id")))

        if candidates:
            candidates.sort()
            result = (candidates[0][2], media_type)
            _tmdb_cache[cache_key] = result
            return result

    _tmdb_cache[cache_key] = (None, None)
    return None, None


def tmdb_watch_providers(tmdb_id, media_type, country=None):
    if country is None:
        country = TMDB_COUNTRY
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/watch/providers?api_key={TMDB_API_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None, None
    flatrate = ((data.get("results") or {}).get(country) or {}).get("flatrate") or []
    found = []
    for provider in flatrate:
        name = provider.get("provider_name", "")
        tag  = TMDB_PROVIDERS.get(name)
        if not tag:
            for pname, ptag in TMDB_PROVIDERS.items():
                if pname.lower() in name.lower():
                    tag = ptag
                    break
        if tag and tag not in found:
            found.append(tag)
    if len(found) == 1:   return found[0], "WEB-DL"
    elif len(found) > 1:  return None, "WEB-DL"
    return None, None


def tmdb_lookup(title, year, candidates=None, is_series=False):
    cache_key = f"{title}|{year}|{is_series}|{TMDB_COUNTRY}"
    if cache_key in _tmdb_full_cache:
        return _tmdb_full_cache[cache_key]
    _p(f"  🎬 TMDB : recherche «{title}» ({year or '?'})...")
    time.sleep(0.25)
    tmdb_id, media_type = tmdb_search(title, year, is_series)
    if not tmdb_id:
        _p("  ⚠️  TMDB : titre non trouvé")
        _tmdb_full_cache[cache_key] = (None, None)
        return None, None
    service, source = tmdb_watch_providers(tmdb_id, media_type)
    if service:
        _p(f"  ✅ TMDB : trouvé sur {service}")
        _tmdb_full_cache[cache_key] = (service, source)
        return service, source
    elif source == "WEB-DL":
        _p("  ⚠️  TMDB : plusieurs services — ambiguïté")
        _tmdb_full_cache[cache_key] = (None, "WEB-DL")
        return None, "WEB-DL"
    else:
        _p("  ⚠️  TMDB : aucun provider CA")
        _tmdb_full_cache[cache_key] = (None, None)
        return None, None


def tmdb_episode_title(series_title, season_num, ep_num, year=None):
    cache_key = f"ep|{series_title}|S{season_num:02d}E{ep_num:02d}"
    if cache_key in _tmdb_full_cache:
        return _tmdb_full_cache[cache_key]
    tmdb_id, _ = tmdb_search(series_title, year, is_series=True)
    if not tmdb_id:
        _tmdb_full_cache[cache_key] = None
        return None
    ep_name = None
    for lang in ["fr-CA", "fr-FR", "en-US"]:
        url = (f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season_num}"
               f"/episode/{ep_num}?api_key={TMDB_API_KEY}&language={lang}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            name = (data.get("name") or "").strip()
            if name and not GENERIC_EPISODE_TITLES.match(name):
                ep_name = name
                break
        except Exception:
            continue
    if ep_name:
        result = re.sub(r'\s+', ' ', re.sub(r"[^\w\s\-']", ' ', to_ascii(ep_name, keep_apostrophe=True, keep_accents=True)).strip()).strip()
        _tmdb_full_cache[cache_key] = result
        return result
    _tmdb_full_cache[cache_key] = None
    return None


# ─── JustWatch ────────────────────────────────────────────────────────────────

def justwatch_lookup(title, year, candidates, country=None):
    if country is None:
        country = JUSTWATCH_COUNTRY
    key = f"{title}|{year}|{country}"
    if key in _jw_cache:
        return _jw_cache[key]
    _p(f"  🔍 JustWatch : recherche «{title}» ({year or '?'})...")
    query = """
    query Search($searchTitlesFilter: TitleFilter!, $country: Country!, $language: Language!) {
      popularTitles(country: $country, filter: $searchTitlesFilter, first: 10) {
        edges {
          node {
            ... on Movie { content(country: $country, language: $language) { title originalReleaseYear }
              offers(country: $country, platform: WEB) { monetizationType package { clearName } } }
            ... on Show { content(country: $country, language: $language) { title originalReleaseYear }
              offers(country: $country, platform: WEB) { monetizationType package { clearName } } }
          }
        }
      }
    }"""
    payload = json.dumps({
        "query": query,
        "variables": {"searchTitlesFilter": {"searchQuery": title}, "country": country, "language": "fr"},
    }).encode("utf-8")
    try:
        req = urllib.request.Request("https://apis.justwatch.com/graphql", data=payload,
                                     headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
                                     method="POST")
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        _p(f"  ⚠️  JustWatch inaccessible : {e}")
        _jw_cache[key] = None
        return None
    time.sleep(0.4)
    edges = ((data.get("data") or {}).get("popularTitles") or {}).get("edges") or []
    title_norm = re.sub(r'[^a-z0-9]', '', title.lower())
    for edge in edges:
        node    = edge.get("node", {})
        content = node.get("content", {})
        node_year  = content.get("originalReleaseYear")
        node_title = re.sub(r'[^a-z0-9]', '', (content.get("title") or "").lower())
        if year and node_year:
            try:
                if abs(int(node_year) - int(year)) > 1: continue
            except ValueError:
                pass
        if title_norm not in node_title and node_title not in title_norm: continue
        found = set()
        for offer in node.get("offers", []):
            if offer.get("monetizationType") in ("FLATRATE", "FLATRATE_AND_BUY"):
                pkg = (offer.get("package") or {}).get("clearName", "").lower().strip()
                for tag, jw_names in JUSTWATCH_PROVIDERS.items():
                    for jw_name in jw_names:
                        if pkg.startswith(jw_name.lower()):
                            found.add(tag)
                            break
        if len(found) == 1:
            result = found.pop()
            _p(f"  ✅ JustWatch : trouvé sur {JUSTWATCH_PROVIDERS.get(result, result)}")
            _jw_cache[key] = result
            return result
        elif len(found) > 1:
            _p(f"  ⚠️  JustWatch : disponible sur plusieurs services — ambigu")
            _jw_cache[key] = None
            return None
    _p("  ⚠️  JustWatch : aucun résultat concluant")
    _jw_cache[key] = None
    return None


def justwatch_series_year(series_title, country=None):
    if country is None:
        country = JUSTWATCH_COUNTRY
    title_clean = series_title.strip()
    _p(f"  📅 JustWatch : année de «{title_clean}»...")
    query = """
    query Search($searchTitlesFilter: TitleFilter!, $country: Country!, $language: Language!) {
      popularTitles(country: $country, filter: $searchTitlesFilter, first: 5) {
        edges { node { ... on Show {
          content(country: $country, language: $language) { title originalReleaseYear }
        }}}
      }
    }"""
    payload = json.dumps({
        "query": query,
        "variables": {"searchTitlesFilter": {"searchQuery": title_clean}, "country": country, "language": "en"},
    }).encode("utf-8")
    try:
        req = urllib.request.Request("https://apis.justwatch.com/graphql", data=payload,
                                     headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
                                     method="POST")
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        _p(f"  ⚠️  JustWatch inaccessible : {e}")
        return None
    time.sleep(0.4)
    edges = ((data.get("data") or {}).get("popularTitles") or {}).get("edges") or []
    title_norm = re.sub(r'[^a-z0-9]', '', title_clean.lower())
    for edge in edges:
        node    = edge.get("node", {})
        content = node.get("content")
        if not content: continue
        node_title = re.sub(r'[^a-z0-9]', '', (content.get("title") or "").lower())
        node_year  = content.get("originalReleaseYear")
        if title_norm in node_title or node_title in title_norm:
            if node_year:
                _p(f"  ✅ Année trouvée : {node_year}")
                return str(node_year)
    _p(f"  ⚠️  Année non trouvée pour «{title_clean}»")
    return None


# ─── Parsing du nom ───────────────────────────────────────────────────────────

_SERVICE_NAME_ALT = '|'.join(
    re.escape(k) for k in sorted(
        {s.strip() for s in list(SERVICE_TAGS) + list(TMDB_PROVIDERS) if s.strip()},
        key=len, reverse=True
    )
)
_TRAILING_TECH_RE = re.compile(
    r'(?:^|\s)(?:(?:' + _SERVICE_NAME_ALT + r')\s+)?'
    r'(?:\d{3,4}p|WEB-DL|WEBRip|BluRay|DVDRip|BDRip|HDTV|MULTi|REMUX)(?=[\s.\-]|$)',
    re.IGNORECASE
)


def _strip_trailing_tech_tags(text):
    """
    Coupe `text` au premier tag technique rencontré (résolution, source, MULTi, REMUX...),
    en absorbant aussi un nom de plateforme (Netflix, Amazon...) juste avant ce tag.
    Évite qu'un titre (film ou épisode) n'embarque les infos de release qui le suivent
    dans le nom d'origine, ex. "Sleep of the Just - Netflix WEB-DL 1080p AV1..." → "Sleep of the Just".
    """
    m = _TRAILING_TECH_RE.search(text)
    if m and m.start() > 0:
        cut = text[:m.start()].strip()
        # Drop a dangling separator left by the cut (e.g. "Sleep of the Just -" → "Sleep of the Just")
        cut = re.sub(r'[\s\-]+$', '', cut).strip()
        return cut or text
    return text


def parse_original_name(stem):
    # Strip edition text from the stem first so it doesn't bleed into the title
    clean_stem, edition = strip_edition(stem)

    team_name = None
    _TECH_TAGS_RE = r'(?:2160|1080|720|576|480|432|360)p|WEB-DL|WEBRip|BluRay|REMUX|DVDRip|BDRip|HDTV'
    if re.search(_TECH_TAGS_RE, clean_stem, re.IGNORECASE):
        _last_tech = max((m.end() for m in re.finditer(_TECH_TAGS_RE, clean_stem, re.IGNORECASE)), default=None)
        if _last_tech is not None:
            _after_tech = clean_stem[_last_tech:]
            _team_m = re.search(r'-([A-Za-z][A-Za-z0-9]{1,7})(?:\.\w+)?$', _after_tech)
            if _team_m:
                candidate = _team_m.group(1)
                if (candidate.isupper() or (len(candidate) <= 4 and candidate[0].isupper()) or
                        re.match(r'^[A-Z][A-Z0-9]+$', candidate)):
                    team_name = candidate

    s = re.sub(r'[_]', ' ', clean_stem)
    if team_name:
        s = re.sub(re.escape('-' + team_name) + r'$', '', s)
    # Safety: strip any remaining explicit edition markers not caught by strip_edition
    s = re.sub(r'\{edition-[^}]+\}', ' ', s)
    s = re.sub(r'\[[^\]]+\]', ' ', s)

    subtitle = None
    _subtitle_year = None
    _informal_ep_num = None
    _informal_ep_title = None
    has_episode = bool(re.search(r'[Ss]\d{1,2}[Ee]\d{1,3}', s))
    # YouTube-style: "# Épisode 01", "Épisode 3", "Episode 12", "Ep 04"
    _yt_ep_m = re.search(
        r'(?:[#\s]+)(?:[Éé]pisode|[Ee]pisode|[Ee][Pp])[\s#._-]*(\d+)',
        s, re.IGNORECASE
    ) if not has_episode else None
    if not has_episode:
        dash_m = re.search(r' - (.+)$', s)
        if dash_m:
            subtitle_raw = dash_m.group(1).strip()
            if subtitle_raw and not re.match(r'^(19|20)\d{2}$', subtitle_raw):
                # Don't treat as subtitle if it contains resolution tags (e.g. "Subtitle 1080p BluRay")
                if not re.search(r'(?:2160|1080|720|576|480|432|360)p', subtitle_raw, re.IGNORECASE):
                    subtitle = to_ascii(re.sub(r"[^\w\s\-']", ' ', subtitle_raw).strip(), keep_apostrophe=True, keep_accents=True)
                    subtitle = re.sub(r'\s+', ' ', subtitle).strip()
                    if len(subtitle) > 60:
                        subtitle = subtitle[:60].rstrip()
                    # Extract trailing year from subtitle (e.g. "Nice Dreams 1981", "Last Jedi 2017")
                    _sub_yr_m = re.search(r'\s+((?:19|20)\d{2})\s*$', subtitle)
                    if _sub_yr_m:
                        _subtitle_year = _sub_yr_m.group(1)
                        subtitle = subtitle[:_sub_yr_m.start()].strip()
                else:
                    # Subtitle has resolution tags — check for YouTube-style "Ep N Title 1080p ..." pattern
                    _ep_fmt = re.match(
                        r'^Ep\s+(\d+)\s+(.*?)(?=\s+\d{3,4}p|\s+WEB-DL|\s+WEBRip|\s+BluRay|\s+HDTV|$)',
                        subtitle_raw, re.IGNORECASE
                    )
                    if _ep_fmt:
                        _informal_ep_num = int(_ep_fmt.group(1))
                        _informal_ep_title = _ep_fmt.group(2).strip() or None
            elif subtitle_raw and re.match(r'^(19|20)\d{2}$', subtitle_raw):
                # subtitle_raw is just a bare year (e.g. "- 2003" left after strip_edition removed the real subtitle)
                _subtitle_year = subtitle_raw
            s = s[:dash_m.start()].strip()

    yr_paren = re.search(r'[\(\[]((?:19|20)\d{2})[\)\]]', s)
    explicit_year = yr_paren.group(1) if yr_paren else None
    s = re.sub(r'\(\s*((19|20)\d{2})\s*\)', r'\1', s)
    s = re.sub(r'\[\s*((19|20)\d{2})\s*\]', r'\1', s)

    ep_m   = re.search(r'[Ss](\d{1,2})[Ee](\d{1,3})', s)
    ep_tag = None
    year   = None
    ep_title = None
    raw    = s

    if ep_m:
        ep_tag = f"S{ep_m.group(1).zfill(2)}E{ep_m.group(2).zfill(2)}"
        raw    = s[:ep_m.start()]
        if explicit_year:
            # L'année (ex. "(2019)") a déjà perdu ses parenthèses ci-dessus ; elle ne doit
            # pas rester collée au titre de série — Plex la porte via le dossier de la série.
            _yr_in_raw = re.search(r'\b' + re.escape(explicit_year) + r'\b', raw)
            if _yr_in_raw:
                raw = raw[:_yr_in_raw.start()]
        after  = s[ep_m.end():].strip().lstrip('- ').strip()
        after  = _strip_trailing_tech_tags(after)
        if after and not GENERIC_EPISODE_TITLES.match(after.strip()):
            ep_title = re.sub(r'\s+', ' ', re.sub(r"[^\w\s\-']", ' ', after).strip()).strip()
        year = explicit_year
    elif _yt_ep_m is not None:
        ep_tag = f"S01E{int(_yt_ep_m.group(1)):02d}"
        # Cut title at the episode marker position to isolate the series name
        raw = s[:_yt_ep_m.start()].strip()
    elif _informal_ep_num is not None:
        ep_tag   = f"S01E{_informal_ep_num:02d}"
        ep_title = _informal_ep_title
        # raw stays as s (the series title portion)
    else:
        if explicit_year:
            yr_m = re.search(r'\b' + explicit_year + r'\b', s)
            if yr_m:
                year   = explicit_year
                ep_tag = explicit_year
                raw    = s[:yr_m.start()]
        else:
            yr_m = re.search(r'\b((?:19|20)\d{2})\b', s)
            if yr_m:
                before = s[:yr_m.start()].strip()
                if before:
                    year   = yr_m.group(0)
                    ep_tag = yr_m.group(0)
                    raw    = before

    # Use year from subtitle as fallback when no year found in the title section
    if _subtitle_year and not year and not ep_m:
        year   = _subtitle_year
        ep_tag = _subtitle_year

    # Strip resolution/source tags that leaked into the title part
    # (e.g. "Dragonheart 1080p MULTi..." → "Dragonheart", "Kill Bill Vol 1 404p..." → "Kill Bill Vol 1")
    raw = _strip_trailing_tech_tags(raw) or raw

    raw = re.sub(r'[\(\[\{][^\)\]\}]{0,30}[\)\]\}]', ' ', raw)
    raw = re.sub(r"[^\w\s\-']", ' ', raw)
    raw = re.sub(r'\s+', ' ', raw).strip()
    raw = re.sub(r'\s+-\s+', ' ', raw)
    title = re.sub(r'\s+', ' ', raw.strip()).strip()
    title = to_ascii(title, keep_apostrophe=True, keep_accents=True)
    if ep_title:
        ep_title = to_ascii(ep_title, keep_apostrophe=True, keep_accents=True)
    return title, ep_tag, year, ep_title, edition, subtitle, team_name


def build_scene_name(title, episode_tag, ep_title, resolution, service, source,
                     video_codec, audio_codec, audio_channels, hdr, team, lang_tag=None,
                     edition=None, subtitle=None, team_name=None):
    is_episode = bool(episode_tag and re.match(r'S\d{2}E\d+', str(episode_tag)))
    # For movies: subtitle is part of the title section (before year/resolution)
    # so Plex can see the full distinguishing title.
    movie_subtitle = subtitle if (subtitle and not is_episode) else None

    parts = []
    if title:          parts.append(title)
    if movie_subtitle: parts.append("- " + movie_subtitle)
    if episode_tag:    parts.append(episode_tag)
    if ep_title:       parts.append(ep_title)
    if resolution:     parts.append(resolution)
    if service and source: parts.append(f"{service} {source}")
    elif source:       parts.append(source)
    if lang_tag:       parts.append(lang_tag)
    if hdr:            parts.append(hdr)
    if audio_codec and audio_channels: parts.append(f"{audio_codec} {audio_channels}")
    elif audio_codec:  parts.append(audio_codec)
    if video_codec and team: parts.append(f"{video_codec}-{team}")
    elif video_codec:  parts.append(video_codec)
    elif team:         parts.append(f"-{team}")

    result = " ".join(parts)
    if subtitle and is_episode:
        ep_clean  = re.sub(r'\s+', ' ', (ep_title or "")).strip().upper()
        sub_clean = re.sub(r'\s+', ' ', subtitle).strip().upper()
        if sub_clean != ep_clean:
            result = result + " - " + subtitle
    if edition:
        result = result + " {edition-" + edition + "}"
    if team_name:
        result = result + "-" + team_name
    return result


def convert_dots_to_spaces(stem):
    s = stem.replace(".", " ")
    restores = [
        (r'\bWEB DL\b', 'WEB-DL'), (r'\bBluRay REMUX\b', 'BluRay.REMUX'),
        (r'\bDVD REMUX\b', 'DVD.REMUX'), (r'\bDTS-HD MA\b', 'DTS-HD.MA'),
        (r'\bTrueHD Atmos\b', 'TrueHD.Atmos'), (r'\bEAC3 Atmos\b', 'EAC3.Atmos'),
        (r'\b((?:TrueHD\.Atmos|EAC3\.Atmos|Atmos))-([A-Z][A-Za-z0-9]{1,7})\b', r'\1 -\2'),
        (r'([A-Za-z])([257]) ([01])\b', r'\1\2.\3'),
        (r'\b([257]) ([01])\b', r'\1.\2'),
        (r'\bHDR10 Plus\b', 'HDR10Plus'),
    ]
    for pattern, replacement in restores:
        s = re.sub(pattern, replacement, s, flags=re.IGNORECASE)
    s = re.sub(r' +', ' ', s).strip()
    return to_ascii(s, keep_apostrophe=True, keep_accents=True)


def is_already_scene_name(stem):
    stem_check = re.sub(r'\{edition-[^}]+\}', '', stem)
    if re.search(r'(?:^|\s)(2160|1080|720|576|480|468|432|360)p(?:\s|$)', stem_check):
        return True
    if re.search(r'\.(2160|1080|720|576|480)p\.', stem_check + "."):
        return True
    stem_upper = stem_check.upper()
    for tag in KNOWN_SERVICE_TAGS:
        if re.search(r'(?:^|[\s\.\-])' + re.escape(tag) + r'(?:[\s\.\-]|$)', stem_upper):
            return True
    return False


def get_season_number(episode_tag):
    m = re.match(r'S(\d{2})E\d+', episode_tag)
    return int(m.group(1)) if m else None


def get_season_from_dir(dir_name):
    m = SEASON_DIR_PATTERN.match(dir_name)
    if not m: return None
    num = m.group(2) or m.group(3)
    return int(num) if num else None


def detect_series_structure(filepath, episode_tag):
    parent      = filepath.parent
    grandparent = parent.parent
    if get_season_from_dir(parent.name) is not None:
        return grandparent, parent
    return parent, None


def series_has_year(series_dir):
    return bool(SERIES_YEAR_PATTERN.search(series_dir.name))


def detect_audio_language_tag(audio_tracks, sub_tracks):
    if not audio_tracks: return None
    langs       = [t.get("lang", "").lower() for t in audio_tracks]
    langs_clean = [l for l in langs if l]
    if not langs_clean: return None

    has_french  = any(l in ("fr", "fre", "fra", "fr-fr", "fr-ca", "fr-be") for l in langs_clean)
    has_fr_ca   = any(l in ("fr-ca",) for l in langs_clean)
    has_english = any(l in ("en", "eng") for l in langs_clean)

    sub_langs    = [s.lower() for s in sub_tracks]
    has_fr_subs  = any(s in ("fr", "fre", "fra", "fr-ca", "fr-fr") for s in sub_langs)

    if len(langs_clean) == 1:
        lang = langs_clean[0]
        if lang in ("en", "eng"): return None
        if lang in ("fr-ca",): return "VFQ"
        if lang in ("fr", "fre", "fra", "fr-fr"): return "VFF"
        return LANG_TAGS.get(lang)

    if has_french and has_english:
        return "MULTi.VFQ" if has_fr_ca else "MULTi"
    if has_french and not has_english:
        return "VFQ" if has_fr_ca else "VFF"
    if not has_french and len(langs_clean) == 1 and has_fr_subs:
        return "VOSTFR"
    if has_english and not has_french and has_fr_subs and len(langs_clean) == 1:
        return "VOSTFR"
    if len(langs_clean) > 1:
        return "MULTi"
    return None


def parse_encoded_date(date_str):
    if not date_str: return None
    import datetime
    s = date_str.strip()
    for prefix in ("UTC ", "utc "):
        if s.startswith(prefix): s = s[len(prefix):]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(s[:len(fmt)], fmt).timestamp()
        except ValueError:
            continue
    return None


def set_file_date(filepath, timestamp):
    if not timestamp: return
    try:
        import os
        os.utime(filepath, (timestamp, timestamp))
    except Exception:
        pass


def get_or_fetch_series_year(series_dir, no_justwatch):
    name = series_dir.name
    if series_has_year(series_dir):
        return None, None
    if name in _series_year_cache:
        return _series_year_cache[name]
    if no_justwatch:
        _series_year_cache[name] = (None, None)
        return None, None

    tmdb_id, _ = tmdb_search(name, None, is_series=True)
    if tmdb_id:
        url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            first_air = (data.get("first_air_date") or "")[:4]
            if first_air:
                new_name = f"{name} ({first_air})"
                _series_year_cache[name] = (new_name, first_air)
                _p(f"  ✅ Année trouvée : {first_air} (TMDB)")
                return new_name, first_air
        except Exception:
            pass

    year = justwatch_series_year(name)
    if year:
        new_name = f"{name} ({year})"
        _series_year_cache[name] = (new_name, year)
        return new_name, year
    _series_year_cache[name] = (None, None)
    return None, None


def handle_series_folders(filepath, episode_tag, no_justwatch, dry_run=False, display_name=None):
    if not episode_tag or not re.match(r'S\d{2}E\d+', episode_tag):
        return filepath
    season_num = get_season_number(episode_tag)
    if season_num is None:
        return filepath

    season_dir_name = f"Season {season_num:02d}"
    series_dir, current_season_dir = detect_series_structure(filepath, episode_tag)

    new_series_name, _ = get_or_fetch_series_year(series_dir, no_justwatch)
    if new_series_name:
        new_series_dir = series_dir.parent / new_series_name
        if not new_series_dir.exists():
            _p(f"  📁 Série      : «{series_dir.name}» → «{new_series_name}»")
            if not dry_run:
                series_dir.rename(new_series_dir)
                series_dir = new_series_dir
                if current_season_dir:
                    current_season_dir = new_series_dir / current_season_dir.name
                filepath = new_series_dir / filepath.name
        else:
            _p(f"  ⚠️  Dossier «{new_series_name}» existe déjà — série non renommée")

    target_season_dir = series_dir / season_dir_name
    if current_season_dir and current_season_dir.name == season_dir_name:
        return filepath

    if not dry_run:
        try:
            target_season_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            _p(f"  ⚠️  Impossible de créer {target_season_dir} : {e}")
            return filepath

    new_filepath = target_season_dir / filepath.name
    if current_season_dir:
        _p(f"  📂 Saison     : «{current_season_dir.name}» → «{season_dir_name}»")
    else:
        _p(f"  📂 Saison     : → «{season_dir_name}» (création)")

    if not dry_run:
        if not filepath.exists():
            _p(f"  ⚠️  Fichier introuvable : {filepath.name}")
            return filepath
        try:
            filepath.rename(new_filepath)
        except Exception:
            try:
                import shutil
                shutil.move(str(filepath), str(new_filepath))
            except Exception as e:
                _p(f"  ⚠️  Déplacement impossible : {e}")
                return filepath
        if current_season_dir and current_season_dir.exists():
            try:
                current_season_dir.rmdir()
            except OSError:
                pass
        return new_filepath
    else:
        shown_name = display_name or filepath.name
        _p(f"  🔍 [DRY RUN] Déplacement simulé → {target_season_dir / shown_name}")
        return filepath


# ═══════════════════════════════════════════════════════════════════════════════
#  TRAITEMENT D'UN FICHIER
# ═══════════════════════════════════════════════════════════════════════════════

def process_file(filepath, opts, stats):
    """
    opts : SimpleNamespace avec dry_run, no_justwatch, service, source,
           resolution, vcodec, acodec, reprocess, force, team,
           validate, corrupted_dir, output_dir
    stats : dict {renamed, already_ok, errors, corrupted, unidentified}
    """
    filepath = Path(filepath)
    if filepath.suffix.lower() not in VIDEO_EXTENSIONS:
        return

    # Corriger double extension
    stem_check = Path(filepath.stem)
    if stem_check.suffix.lower() in VIDEO_EXTENSIONS:
        new_filepath = filepath.parent / (stem_check.stem + filepath.suffix)
        if not new_filepath.exists() and not opts.dry_run:
            try:
                filepath.rename(new_filepath)
            except Exception:
                import shutil
                shutil.move(str(filepath), str(new_filepath))
            filepath = new_filepath
            _p(f"\n🔧 Double extension corrigée : {new_filepath.name}")
        elif opts.dry_run:
            _p(f"\n🔍 [DRY RUN] Double extension : {filepath.name} → {new_filepath.name}")

    _p(f"\n📄 {filepath.name}")

    if opts.validate:
        is_valid, err = validate_video_integrity(filepath)
        if not is_valid:
            _p(f"  ❌ Corrompu : {err}")
            stats["corrupted"] += 1
            if opts.output_dir:
                sort_output_file(filepath, filepath.name, opts, category="corrupted")
            elif opts.corrupted_dir:
                move_to_corrupted(filepath, opts.corrupted_dir, opts.dry_run)
            return
        _p("  ✅ Intégrité OK (décodage complet)")

    if is_already_scene_name(filepath.stem) and not opts.force:
        if opts.reprocess:
            new_stem = convert_dots_to_spaces(filepath.stem)
            if len(new_stem) > 180:
                new_stem = new_stem[:180].rstrip()
            new_name = new_stem + filepath.suffix.lower()
            new_path = filepath.parent / new_name
            if filepath.name != new_name:
                _p(f"  🔄 Conversion  : {new_name}")
                if not opts.dry_run:
                    if not new_path.exists():
                        try:
                            filepath.rename(new_path)
                        except Exception:
                            import shutil
                            shutil.move(str(filepath), str(new_path))
                        filepath = new_path
                        stats["renamed"] += 1
                        _p("  ✅ Converti !")
                    else:
                        _p("  ⚠️  Fichier cible existe déjà, ignoré.")
                else:
                    _p("  🔍 [DRY RUN] Conversion simulée.")
            else:
                _p("  ✅ Déjà en format espaces — ignoré.")
                stats["already_ok"] += 1
            _, ep_tag_check, _, _, _, _, _ = parse_original_name(filepath.stem)
            if ep_tag_check and re.match(r'S\d{2}E\d+', ep_tag_check):
                handle_series_folders(filepath, ep_tag_check, opts.no_justwatch, dry_run=opts.dry_run, display_name=new_name)
        else:
            _, ep_tag_check, _, _, _, _, _ = parse_original_name(filepath.stem)
            if ep_tag_check and re.match(r'S\d{2}E\d+', ep_tag_check):
                handle_series_folders(filepath, ep_tag_check, opts.no_justwatch, dry_run=opts.dry_run)
            _p("  ✅ Déjà en nomenclature scène — ignoré.")
            stats["already_ok"] += 1
        return

    mi = get_media_info(filepath)
    if mi is None:
        stats["errors"] += 1
        if opts.output_dir:
            sort_output_file(filepath, filepath.name, opts, category="error")
        return

    if opts.force:
        ep_m = re.search(r'[Ss](\d{1,2})[Ee](\d{1,3})', filepath.stem)
        if ep_m:
            # Series: clear title so it's derived from the series folder name
            ep_tag    = f"S{ep_m.group(1).zfill(2)}E{ep_m.group(2).zfill(2)}"
            title     = ""
            year      = None
            ep_title  = None
            edition   = detect_edition(filepath.stem)
            subtitle  = None
            team_name = None
        else:
            # Movies: parse from filename first, then fall back to the embedded
            # MediaInfo title when the filename is junk (e.g. PhotoRec recovery
            # names like "f4260167920.mp4" that carry no usable info).
            title, ep_tag, year, ep_title, edition, subtitle, team_name = parse_original_name(filepath.stem)
            meta_title = (mi.get("meta_title") or "").strip()
            if meta_title and not GENERIC_EPISODE_TITLES.match(meta_title):
                m_title, m_ep_tag, m_year, m_ep_title, m_edition, m_subtitle, m_team_name = parse_original_name(meta_title)
                is_junk_title = not title or GENERIC_EPISODE_TITLES.match(title) or re.match(r'^[a-z]?\d{5,}$', title, re.IGNORECASE)
                if m_title and is_junk_title:
                    _p(f"  🎬 Titre       : «{m_title}» (métadonnées)")
                    title, ep_tag, year = m_title, (m_ep_tag or ep_tag), (m_year or year)
                    ep_title  = ep_title  or m_ep_title
                    edition   = edition   or m_edition
                    subtitle  = subtitle  or m_subtitle
                    team_name = team_name or m_team_name
    else:
        title, ep_tag, year, ep_title, edition, subtitle, team_name = parse_original_name(filepath.stem)

    if not title and ep_tag:
        series_dir, _ = detect_series_structure(filepath, ep_tag) if ep_tag and re.match(r'S\d{2}E\d+', ep_tag) else (filepath.parent, None)
        series_name = re.sub(r'\s*\(\d{4}\)\s*$', '', series_dir.name).strip()
        title = to_ascii(re.sub(r'\s+', ' ', series_name).strip(), keep_apostrophe=True, keep_accents=True)

    is_episode = bool(ep_tag and re.match(r'S\d{2}E\d+', ep_tag))
    if not ep_title and is_episode and mi.get("meta_title"):
        meta = mi["meta_title"].strip()
        # Clean YouTube-style metadata: "S2E1 'Mind on Fire' - Impulse" → "Mind on Fire"
        meta = re.sub(r'^S\d{1,2}E\d{1,3}\s+', '', meta, flags=re.IGNORECASE)
        meta = re.sub(r"^['‘’](.+?)['‘’]\s*$", r'\1', meta)
        meta = _strip_trailing_tech_tags(meta)
        _meta_dash = re.search(r'\s+-\s+(\S.*?)$', meta)
        if _meta_dash:
            _suffix_norm = re.sub(r'[^a-z0-9]', '', _meta_dash.group(1).lower())
            _title_norm  = re.sub(r'[^a-z0-9]', '', title.lower())
            if _suffix_norm and (_suffix_norm in _title_norm or _title_norm in _suffix_norm):
                meta = meta[:_meta_dash.start()].strip()
        series_clean = re.sub(r'[^a-z0-9]', '', title.lower().replace('.', ' '))
        meta_clean   = re.sub(r'[^a-z0-9]', '', meta.lower())
        if meta_clean and meta_clean != series_clean and not GENERIC_EPISODE_TITLES.match(meta):
            ep_title = re.sub(r'\s+', ' ', re.sub(r"[^\w\s\-']", ' ', to_ascii(meta, keep_apostrophe=True, keep_accents=True)).strip()).strip()
            _p(f"  🎬 Épisode     : {ep_title} (métadonnées)")

    if ep_tag and not ep_title and not opts.no_justwatch:
        ep_m_re = re.match(r'S(\d{2})E(\d{2,})', ep_tag)
        if ep_m_re:
            series_name_clean = re.sub(r'\.+', ' ', title).strip()
            s_num = int(ep_m_re.group(1))
            e_num = int(ep_m_re.group(2))
            ep_title = tmdb_episode_title(series_name_clean, s_num, e_num, year)
            if ep_title:
                _p(f"  🎬 Épisode     : {ep_title} (TMDB)")

    resolution     = mi["resolution"]     or opts.resolution or "1080p"
    video_codec    = mi["video_codec"]    or opts.vcodec     or "H264"
    audio_codec    = mi["audio_codec"]    or opts.acodec     or "AAC"
    audio_channels = mi["audio_channels"] or "2.0"
    hdr            = mi["hdr"]

    team = None if opts.force else detect_existing_scene_tag(filepath.stem)
    if team:
        _p(f"  🏷️  Tag existant : -{team}")

    service    = opts.service
    source     = opts.source
    infer_note = ""

    if not service:
        service = detect_service_from_name(filepath, mi)
        if service:
            source = "WEBRip" if service in ("YT", "TUBI", "PLUTO", "GEM", "CTV") else "WEB-DL"

    if not service and not source:
        svc_any, src_any = detect_anystream_service(mi)
        if src_any:
            service = svc_any
            source  = src_any

    if not source:
        svc_app, src_app = detect_source_from_app(mi)
        if src_app:
            service = svc_app
            source  = src_app

    if not service and not source:
        inf_svc, inf_src, confidence, candidates, infer_note = infer_from_codec(mi)
        if confidence in ("certain", "high"):
            service = inf_svc
            source  = inf_src
        elif confidence == "ambiguous" and candidates and not opts.no_justwatch:
            title_clean = title.replace(".", " ")
            is_series   = bool(ep_tag and re.match(r'S\d{2}E\d+', ep_tag))
            svc_tmdb, src_tmdb = tmdb_lookup(title_clean, year, candidates, is_series)
            if svc_tmdb:
                service = svc_tmdb
                source  = src_tmdb
            elif src_tmdb == "WEB-DL":
                jw_result = justwatch_lookup(title_clean, year, candidates)
                service = jw_result if jw_result else None
                source  = inf_src
            else:
                jw_result = justwatch_lookup(title_clean, year, candidates)
                service = jw_result if jw_result else None
                source  = None
        elif confidence == "medium":
            service = inf_svc
            source  = inf_src

    if not service and not source:
        svc_mkv, src_mkv = detect_service_from_mkvmerge(mi)
        if svc_mkv:
            service = svc_mkv
            source  = src_mkv

    _p(f"  🎬 Résolution  : {resolution}")
    _p(f"  🎞️  Vidéo       : {video_codec}{' + ' + hdr if hdr else ''}")
    _p(f"  🔊 Audio       : {audio_codec} {audio_channels}")
    if service:
        _p(f"  🌐 Service     : {service}.{source}")
    else:
        _p("  🌐 Service     : (inconnu — omis)")

    lang_tag = detect_audio_language_tag(mi.get("audio_tracks", []), mi.get("sub_tracks", []))
    if lang_tag:
        _p(f"  🌍 Langue      : {lang_tag}")
    if edition:
        _p(f"  📦 Édition     : {edition}")

    effective_team = opts.team or team_name

    new_stem = build_scene_name(
        title=title, episode_tag=ep_tag, ep_title=ep_title,
        resolution=resolution, service=service, source=source,
        video_codec=video_codec, audio_codec=audio_codec,
        audio_channels=audio_channels, hdr=hdr, team=team,
        lang_tag=lang_tag, edition=edition, subtitle=subtitle,
        team_name=effective_team,
    )
    if len(new_stem) > 180:
        new_stem = new_stem[:180].rstrip()
    new_name = new_stem + filepath.suffix.lower()
    new_path = filepath.parent / new_name

    _p(f"  ✏️  Nouveau nom : {new_name}")

    if opts.output_dir:
        identified, resolved_year = check_identified(title, year, is_episode)
        season_num = get_season_number(ep_tag) if is_episode else None
        category = "renamed" if identified else "unidentified"
        # Pour une série, resolved_year est toujours l'année de première diffusion
        # (voir check_identified) : elle doit primer sur l'année de la saison en
        # cours pour que toutes les saisons tombent dans le même dossier "Titre (Année)".
        folder_year = resolved_year if is_episode else (year or resolved_year)
        dest = sort_output_file(
            filepath, new_name, opts, category=category,
            title=title, year=folder_year, season_num=season_num,
        )
        if dest is None:
            stats["errors"] += 1
            return
        if identified:
            stats["renamed"] += 1
            if not opts.dry_run:
                encoded_ts = parse_encoded_date(mi.get("encoded_date") or "")
                if encoded_ts:
                    set_file_date(dest, encoded_ts)
        else:
            _p("  ⚠️  Non identifié (TMDB)")
            stats["unidentified"] += 1
        return

    if filepath.name == new_name:
        _p("  ✅ Nom déjà correct.")
        if is_episode:
            handle_series_folders(filepath, ep_tag, opts.no_justwatch, dry_run=opts.dry_run, display_name=new_name)
        return

    if new_path.exists():
        _p("  ⚠️  Fichier cible existe déjà, ignoré.")
        return

    if opts.dry_run:
        _p("  🔍 [DRY RUN] Pas de renommage.")
        if is_episode:
            handle_series_folders(filepath, ep_tag, opts.no_justwatch, dry_run=True, display_name=new_name)
        return

    try:
        filepath.rename(new_path)
    except PermissionError as e:
        _p(f"  ❌ Permission refusée : {e}")
        stats["errors"] += 1
        return
    except Exception:
        try:
            import shutil
            shutil.move(str(filepath), str(new_path))
        except Exception as e2:
            _p(f"  ❌ Impossible de renommer : {e2}")
            stats["errors"] += 1
            return

    _p("  ✅ Renommé !")
    stats["renamed"] += 1

    encoded_ts = parse_encoded_date(mi.get("encoded_date") or "")
    if encoded_ts:
        set_file_date(new_path, encoded_ts)

    if is_episode:
        handle_series_folders(new_path, ep_tag, opts.no_justwatch, dry_run=False)


# ═══════════════════════════════════════════════════════════════════════════════
#  WORKER
# ═══════════════════════════════════════════════════════════════════════════════

def _worker(path_str, tmdb_api_key, dry_run, recursive, no_justwatch,
            service, source, resolution, vcodec, acodec, reprocess, force, team,
            country, tmdb_id_override, name_filter, validate, corrupted_dir, output_dir):
    global TMDB_API_KEY, TMDB_COUNTRY, JUSTWATCH_COUNTRY, _forced_tmdb_id

    _tl.log_fn = _rename_log
    _reset_caches()

    TMDB_API_KEY      = tmdb_api_key or ""
    TMDB_COUNTRY      = country.upper() if country else "CA"
    JUSTWATCH_COUNTRY = TMDB_COUNTRY
    _forced_tmdb_id   = tmdb_id_override

    _update(running=True, done=False, total=0, processed=0,
            renamed=0, already_ok=0, errors=0, corrupted=0, unidentified=0, log=[])

    opts = SimpleNamespace(
        dry_run=dry_run, no_justwatch=no_justwatch,
        service=service or None, source=source or None,
        resolution=resolution or None, vcodec=vcodec or None, acodec=acodec or None,
        reprocess=reprocess, force=force, team=team or None,
        validate=validate, corrupted_dir=corrupted_dir or None,
        output_dir=output_dir or None,
    )

    stats = {"renamed": 0, "already_ok": 0, "errors": 0, "corrupted": 0, "unidentified": 0}

    try:
        target = Path(path_str)
        if not target.exists():
            _rename_log(f"❌ Chemin introuvable : {path_str}")
            return

        if target.is_file():
            video_files = [target] if target.suffix.lower() in VIDEO_EXTENSIONS else []
        else:
            pattern = "**/*" if recursive else "*"
            video_files = sorted(
                (f for f in target.glob(pattern) if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS),
                key=lambda f: f.stat().st_mtime,
            )

        if name_filter:
            nf_lower = name_filter.lower()
            video_files = [f for f in video_files if nf_lower in f.stem.lower()]
            _rename_log(f"🔍 Filtre «{name_filter}» → {len(video_files)} fichier(s) correspondant(s)")

        _update(total=len(video_files))
        _rename_log(f"▶ {len(video_files)} fichier(s) trouvé(s) dans «{path_str}»")

        for i, filepath in enumerate(video_files):
            process_file(filepath, opts, stats)
            _update(
                processed=i + 1,
                renamed=stats["renamed"],
                already_ok=stats["already_ok"],
                errors=stats["errors"],
                corrupted=stats["corrupted"],
                unidentified=stats["unidentified"],
            )

        _rename_log(
            f"✔ Terminé — {stats['renamed']} renommé(s), "
            f"{stats['already_ok']} déjà OK, {stats['unidentified']} non identifié(s), "
            f"{stats['corrupted']} corrompu(s), {stats['errors']} erreur(s)"
        )

    except Exception as e:
        _rename_log(f"❌ Erreur fatale : {e}")
        logger.error("[RenameService] Fatal error", exc_info=True)
    finally:
        _update(running=False, done=True)
        _tl.log_fn = None


def start_rename_job(path, tmdb_api_key, dry_run=True, recursive=False,
                     no_justwatch=False, service=None, source=None,
                     resolution=None, vcodec=None, acodec=None,
                     reprocess=False, force=False, team=None,
                     country="CA", tmdb_id_override=None, name_filter=None,
                     validate=False, corrupted_dir=None, output_dir=None):
    t = threading.Thread(
        target=_worker,
        args=(path, tmdb_api_key, dry_run, recursive, no_justwatch,
              service, source, resolution, vcodec, acodec, reprocess, force, team,
              country, tmdb_id_override, name_filter, validate, corrupted_dir, output_dir),
        daemon=True,
    )
    t.start()
