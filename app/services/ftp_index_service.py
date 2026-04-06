import os
import json
import re
import unicodedata
import threading
from datetime import datetime, timedelta
from flask import current_app
from flask import jsonify

from app.config_paths import CACHE_DIR
from app.services.ftp_service import get_ftp_client, ftp_walk_recursive

ftp_index_lock = threading.Lock()

# ─── Cache mémoire par serveur FTP ──────────────────────────────────────────
# _ftp_memory_caches[ftp_id] = { data, mtime, episode_index, series_index, movie_items }
_ftp_memory_caches: dict = {}
_ftp_cache_lock = threading.Lock()

# ─── Statuts par serveur FTP ────────────────────────────────────────────────
# ftp_index_statuses[ftp_id] = snapshot du statut après le dernier scan terminé
ftp_index_statuses: dict = {}


def _get_ftp_index_cached(ftp_id: str):
    """
    Retourne (data, episode_index, series_index, movie_items) pour un serveur FTP donné.
    Recharge depuis le disque uniquement si le fichier a changé.
    """
    path = get_ftp_index_file(ftp_id)

    try:
        mtime = os.path.getmtime(path) if os.path.exists(path) else None
    except OSError:
        mtime = None

    with _ftp_cache_lock:
        cache = _ftp_memory_caches.get(ftp_id, {})
        if cache.get("data") is not None and cache.get("mtime") == mtime:
            return (
                cache["data"],
                cache["episode_index"],
                cache["series_index"],
                cache["movie_items"],
            )

    # Recharger depuis le disque
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {"generated_at": None, "items": []}

    items = data.get("items", [])

    episode_index = {}
    series_index = {}
    movie_items = []

    for item in items:
        media_type = item.get("media_type")
        if media_type == "episode":
            s = item.get("season")
            e = item.get("episode")
            if s is not None and e is not None:
                try:
                    key = (int(s), int(e))
                    episode_index.setdefault(key, []).append(item)
                except (TypeError, ValueError):
                    pass
            nst = item.get("normalized_series_title") or ""
            if nst:
                series_index.setdefault(nst, []).append(item)
        elif media_type == "movie":
            movie_items.append(item)
        elif media_type == "unknown":
            if item.get("season") is None and item.get("episode") is None:
                movie_items.append(item)

    with _ftp_cache_lock:
        _ftp_memory_caches[ftp_id] = {
            "data":          data,
            "mtime":         mtime,
            "episode_index": episode_index,
            "series_index":  series_index,
            "movie_items":   movie_items,
        }

    return data, episode_index, series_index, movie_items


def _invalidate_ftp_memory_cache(ftp_id: str | None = None):
    """Invalide le cache mémoire pour un serveur FTP donné, ou tous si ftp_id=None."""
    with _ftp_cache_lock:
        if ftp_id is None:
            _ftp_memory_caches.clear()
        else:
            _ftp_memory_caches.pop(ftp_id, None)


ftp_index_status = {
    "running": False,
    "phase": "idle",              # idle | scanning | comparing | ready | error | cancelled
    "current_root": None,
    "progress": 0,
    "total": 0,
    "files_found": 0,
    "estimated_total_files": None,
    "estimated_percent": 0.0,
    "comparison_done": 0,
    "comparison_total": 0,
    "comparison_percent": 0.0,
    "finished": False,
    "message": "",
    "cancel_requested": False,
    "started_at": None,           # timestamp float du début du scan courant
    "last_scan_duration_s": None, # durée en secondes du dernier scan terminé
    "last_scan_finished_at": None,# timestamp ISO du dernier scan terminé
}

MOVIE_STOPWORDS = {
    "the", "a", "an", "of", "and", "et", "la", "le", "les",
    "des", "du", "de", "d", "l", "part", "episode", "movie"
}

SERIES_STOPWORDS = {
    "the", "a", "an", "of", "and", "et", "la", "le", "les",
    "des", "du", "de", "d", "l"
}

NOISE_WORDS = {
    "web", "webrip", "webdl", "bluray", "brrip", "dvdrip", "bdrip",
    "hdrip", "hdtv", "remux", "hdlight", "mhd", "dllight", "dl", "ddp",
    "amzn", "nf", "dsnp", "hmax", "pcok", "kanopy", "teleqc",
    "multi", "french", "truefrench", "vf", "vfq", "vf2", "vff", "vfi",
    "vostfr", "vo", "vof", "eng", "english", "fr",
    "x264", "x265", "h264", "h265", "hevc", "avc",
    "aac", "ac3", "dd", "ddp", "ddp5", "ddp51", "truehd", "dts",
    "hdma", "atmos", "10bit", "sdr",
    "1080p", "720p", "2160p", "480p", "480i", "4k",
    "proper", "repack", "readnfo", "extended", "unrated", "uncut",
    "custom", "doc", "ad", "with", "clean", "break", "stv", "imax",
    "fw", "sigma", "ajp69", "byndr", "plissken", "sbr", "flux", "supply",
    "pophd", "lihdl", "johnnygretis", "livromaniac", "slay3r", "tfa",
    "rough", "winks", "kazetv", "playhd", "playweb", "watchable",
    "glados", "ulysse", "notag", "monkee", "sigla", "kitsune",
    "delicious", "rht", "oft", "fhd", "gizmo65", "romkent", "handjob",
    "bzh29", "arcadia", "extreme", "threesome", "dtr", "nogrp",
    "claudeb71", "cannedheat", "sel", "azaze", "eaulive", "preacherman",
    "humungusrepack", "humungus", "darkino", "com", "mtl666", "weedsmoke",
    "animatek", "jacq", "jaqc", "enjoi", "sic", "fck", "wlm", "riper",
    "redisdead", "mehdibleu", "dutch", "chatknight", "canona1", "mu",
    "hone", "the", "decibel",
}

SHOW_HINTS = {
    "tv", "shows", "show", "series", "serie", "series tv", "séries",
    "anime", "episodes", "episodios", "cartoons", "dessins animes",
    "dessins animés", "tele", "télé"
}

MOVIE_HINTS = {
    "films", "film", "movies", "movie", "cinema", "cinéma"
}

SEASON_FOLDER_PATTERNS = [
    re.compile(r"(?i)^season[ ._-]?(\d{1,2})$"),
    re.compile(r"(?i)^saison[ ._-]?(\d{1,2})$"),
    re.compile(r"(?i)^s(\d{1,2})$"),
]

EPISODE_PATTERNS = [
    ("sxe", re.compile(r"(?i)\bS(\d{1,2})[.\- _]?E(\d{1,3})\b")),
    ("nxn", re.compile(r"(?i)\b(\d{1,2})x(\d{1,3})\b")),
    ("season_episode", re.compile(r"(?i)\bSeason[.\- _]?(\d{1,2})[.\- _]*(?:Episode|Ep)[.\- _]?(\d{1,3})\b")),
    ("ep_only", re.compile(r"(?i)\b(?:Episode|Ep)[.\- _]?(\d{1,3})\b")),
]

YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2}|21\d{2})\b")

SERIES_TOKEN_EQUIVALENTS = {
    "zero": "0",
    "zéro": "0",
    "un": "1",
    "une": "1",
    "one": "1",
    "deux": "2",
    "two": "2",
    "trois": "3",
    "three": "3",
    "quatre": "4",
    "four": "4",
    "cinq": "5",
    "five": "5",
    "six": "6",
    "seven": "7",
    "sept": "7",
    "eight": "8",
    "huit": "8",
    "nine": "9",
    "neuf": "9",
    "ten": "10",
    "dix": "10",
}


def _strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", str(text))
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def _roman_to_int_token(tok: str) -> str:
    roman_map = {
        "ii": "2",
        "iii": "3",
        "iv": "4",
        "v": "5",
        "vi": "6",
        "vii": "7",
        "viii": "8",
        "ix": "9",
        "x": "10",
    }
    return roman_map.get(tok, tok)


def _clean_separators(text: str) -> str:
    text = text.replace("&", " and ")
    text = text.replace("'", " ")  # apostrophe → espace pour préserver les tokens (c'est → c est)
    text = re.sub(r"[._\-]+", " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _clean_path_piece(text: str) -> str:
    if not text:
        return ""
    text = _strip_accents(str(text)).lower()
    text = text.replace("&", " and ")
    text = text.replace("'", " ")  # apostrophe → espace
    text = re.sub(r"[._\-]+", " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _split_path_parts(path: str):
    if not path:
        return []
    norm = str(path).replace("\\", "/")
    return [_clean_path_piece(p) for p in norm.split("/") if p.strip()]


def _is_season_folder(name: str):
    if not name:
        return None

    clean = _clean_path_piece(name)

    for pat in SEASON_FOLDER_PATTERNS:
        m = pat.search(clean)
        if m:
            try:
                season = int(m.group(1))
                if 0 < season < 100:
                    return season
            except Exception:
                return None
    return None


def _remove_extension(filename: str) -> str:
    return os.path.splitext(filename or "")[0]


def _remove_release_group_suffix(title: str) -> str:
    if not title:
        return ""
    return re.sub(r"-[A-Za-z0-9]+$", " ", title)


def _remove_episode_markers(text: str) -> str:
    if not text:
        return ""

    out = str(text)

    patterns = [
        r"(?i)\bS\d{1,2}[.\- _]?E\d{1,3}\b",
        r"(?i)\b\d{1,2}x\d{1,3}\b",
        r"(?i)\bSeason[.\- _]?\d{1,2}[.\- _]*(?:Episode|Ep)[.\- _]?\d{1,3}\b",
        r"(?i)\b(?:Episode|Ep)[.\- _]?\d{1,3}\b",
        r"(?i)\bE\d{1,3}\b",
    ]
    for pat in patterns:
        out = re.sub(pat, " ", out)

    out = re.sub(r"\s+", " ", out).strip()
    return out


def normalize_title(title):
    if not title:
        return ""

    title = _remove_release_group_suffix(title)
    title = _strip_accents(title).lower()
    title = re.sub(r"\[[^\]]*\]", " ", title)
    title = re.sub(r"\([^\)]*\)", " ", title)
    title = _clean_separators(title)
    title = re.sub(r"\b\d+\.\d+\b", " ", title)
    title = title.replace("½", " demi")

    raw_tokens = title.split()
    tokens = []

    for tok in raw_tokens:
        if re.fullmatch(r"(19\d{2}|20\d{2}|21\d{2})", tok):
            continue

        tok = _roman_to_int_token(tok)

        if re.fullmatch(r"(?=.*[a-z])(?=.*\d)[a-z0-9]+", tok):
            continue

        if tok in NOISE_WORDS:
            continue

        tok = re.sub(r"^(?:s|e)\d{1,3}$", "", tok)
        tok = tok.strip()

        if not tok:
            continue

        tokens.append(tok)

    return " ".join(tokens).strip()


def normalize_series_title(title):
    if not title:
        return ""
    cleaned = _remove_episode_markers(_remove_extension(title))
    return normalize_title(cleaned)


def normalize_series_for_matching(title):
    norm = normalize_series_title(title)
    if not norm:
        return ""

    tokens = []
    for tok in norm.split():
        tok = SERIES_TOKEN_EQUIVALENTS.get(tok, tok)
        tok = _roman_to_int_token(tok)
        tokens.append(tok)

    return " ".join(tokens).strip()


def title_tokens(title):
    norm = normalize_title(title)
    if not norm:
        return []

    return [
        tok for tok in norm.split()
        if tok and tok not in MOVIE_STOPWORDS
    ]


def series_title_tokens(title):
    norm = normalize_series_title(title)
    if not norm:
        return []

    return [
        tok for tok in norm.split()
        if tok and tok not in MOVIE_STOPWORDS
    ]


def series_match_tokens(title):
    norm = normalize_series_for_matching(title)
    if not norm:
        return []

    return [
        tok for tok in norm.split()
        if tok and tok not in SERIES_STOPWORDS
    ]


def get_title_variants(title):
    variants = []

    raw = str(title or "").strip()
    full = normalize_title(raw)

    if full:
        variants.append((full, "full"))

    separators = [" - ", ": ", " / "]
    for sep in separators:
        if sep in raw:
            left = raw.split(sep, 1)[0].strip()
            left_norm = normalize_title(left)
            if left_norm and left_norm != full:
                variants.append((left_norm, "truncated"))

    return variants


def token_set_similarity(a_tokens, b_tokens):
    if not a_tokens or not b_tokens:
        return 0.0
    a = set(a_tokens)
    b = set(b_tokens)
    inter = len(a & b)
    denom = max(len(a), len(b))
    return inter / denom if denom else 0.0


def token_containment(a_tokens, b_tokens):
    if not a_tokens:
        return 0.0
    a = set(a_tokens)
    b = set(b_tokens)
    return len(a & b) / len(a)


def extract_year(text):
    if not text:
        return None
    m = YEAR_PATTERN.search(str(text))
    return int(m.group(1)) if m else None


def _count_noise_words(text: str) -> int:
    if not text:
        return 0
    cleaned = _clean_path_piece(text)
    tokens = set(cleaned.split())
    return sum(1 for t in tokens if t in NOISE_WORDS)


def parse_episode_filename(name: str, path: str = ""):
    raw_name = _remove_extension(name or "")
    raw_path = path or ""

    for pattern_name, pat in EPISODE_PATTERNS:
        m = pat.search(raw_name)
        if m:
            if pattern_name in ("sxe", "nxn", "season_episode"):
                season = int(m.group(1))
                episode = int(m.group(2))
            elif pattern_name == "ep_only":
                season = None
                episode = int(m.group(1))
            else:
                season = None
                episode = None

            title_guess = normalize_series_title(raw_name)
            return {
                "season": season,
                "episode": episode,
                "pattern": pattern_name,
                "title_guess": title_guess,
            }

    for pattern_name, pat in EPISODE_PATTERNS:
        m = pat.search(raw_path)
        if m:
            if pattern_name in ("sxe", "nxn", "season_episode"):
                season = int(m.group(1))
                episode = int(m.group(2))
            elif pattern_name == "ep_only":
                season = None
                episode = int(m.group(1))
            else:
                season = None
                episode = None

            title_guess = normalize_series_title(raw_name)
            return {
                "season": season,
                "episode": episode,
                "pattern": f"path_{pattern_name}",
                "title_guess": title_guess,
            }

    return {
        "season": None,
        "episode": None,
        "pattern": None,
        "title_guess": normalize_series_title(raw_name),
    }


def parse_movie_filename(name: str, path: str = ""):
    raw_name = _remove_extension(name or "")
    year = extract_year(raw_name) or extract_year(path)
    normalized = normalize_title(raw_name)

    return {
        "year": year,
        "title_guess": normalized,
    }


def _guess_series_title_from_path(path: str, root: str = ""):
    if not path:
        return None

    full_parts_raw = [p for p in str(path).replace("\\", "/").split("/") if p.strip()]
    if not full_parts_raw:
        return None

    folder_parts_raw = full_parts_raw[:-1]
    if not folder_parts_raw:
        return None

    root_parts_raw = [p for p in str(root or "").replace("\\", "/").split("/") if p.strip()]
    root_len = len(root_parts_raw)

    rel_parts_raw = folder_parts_raw[root_len:] if root_len <= len(folder_parts_raw) else folder_parts_raw
    rel_parts_clean = [_clean_path_piece(p) for p in rel_parts_raw]

    if not rel_parts_raw:
        return None

    filtered = []
    for raw, clean in zip(rel_parts_raw, rel_parts_clean):
        if clean in SHOW_HINTS or clean in MOVIE_HINTS:
            continue
        filtered.append((raw, clean))

    if not filtered:
        return None

    if _is_season_folder(filtered[-1][1]) is not None:
        if len(filtered) >= 2:
            return filtered[-2][0].strip()
        return None

    return filtered[-1][0].strip()


def series_title_match_score(wanted_title, candidate_title):
    wanted_norm = normalize_series_for_matching(wanted_title)
    candidate_norm = normalize_series_for_matching(candidate_title)

    wanted_tokens = series_match_tokens(wanted_title)
    candidate_tokens = series_match_tokens(candidate_title)

    if not wanted_tokens or not candidate_tokens:
        return {
            "score": 0,
            "containment": 0.0,
            "similarity": 0.0,
            "common_tokens": [],
            "wanted_norm": wanted_norm,
            "candidate_norm": candidate_norm,
        }

    wanted_set = set(wanted_tokens)
    candidate_set = set(candidate_tokens)
    common = sorted(wanted_set & candidate_set)

    containment = len(common) / len(wanted_set) if wanted_set else 0.0
    similarity = len(common) / max(len(wanted_set), len(candidate_set)) if max(len(wanted_set), len(candidate_set)) else 0.0

    score = 0

    if wanted_norm == candidate_norm:
        score += 160

    if wanted_set.issubset(candidate_set):
        score += 100

    score += int(containment * 90)
    score += int(similarity * 50)

    if len(wanted_set) <= 2:
        if len(common) >= 1:
            score += 25
        if containment >= 0.5:
            score += 20

    if wanted_norm and candidate_norm and wanted_norm in candidate_norm:
        score += 12

    return {
        "score": score,
        "containment": containment,
        "similarity": similarity,
        "common_tokens": common,
        "wanted_norm": wanted_norm,
        "candidate_norm": candidate_norm,
    }


def detect_media_type(name: str, path: str = "", root: str = ""):
    raw_name = name or ""
    raw_path = path or ""
    name_no_ext = _remove_extension(raw_name)

    path_parts = _split_path_parts(raw_path)
    root_parts = _split_path_parts(root)

    episode_score = 0
    movie_score = 0
    reasons = []

    parsed_ep = parse_episode_filename(raw_name, raw_path)
    parsed_movie = parse_movie_filename(raw_name, raw_path)

    season = parsed_ep["season"]
    episode = parsed_ep["episode"]
    year = parsed_movie["year"]

    normalized_title = parsed_movie["title_guess"]

    if parsed_ep["episode"] is not None:
        if parsed_ep["pattern"] in ("sxe", "nxn", "season_episode"):
            episode_score += 140
            reasons.append(f"pattern episode fort: {parsed_ep['pattern']}")
        elif parsed_ep["pattern"] in ("ep_only", "path_ep_only"):
            episode_score += 80
            reasons.append(f"pattern episode faible: {parsed_ep['pattern']}")
        else:
            episode_score += 100
            reasons.append(f"pattern episode: {parsed_ep['pattern']}")

    if parsed_ep["season"] is not None and parsed_ep["episode"] is not None:
        episode_score += 35
        reasons.append("saison+episode détectés")

    if parsed_ep["episode"] is not None and parsed_ep["season"] is None:
        episode_score += 10
        reasons.append("episode détecté sans saison")

    for part in path_parts:
        s = _is_season_folder(part)
        if s is not None:
            episode_score += 40
            if season is None:
                season = s
            reasons.append(f"dossier saison détecté: {part}")
            break

    for part in path_parts:
        if part in SHOW_HINTS:
            episode_score += 30
            reasons.append(f"dossier série: {part}")
        if part in MOVIE_HINTS:
            movie_score += 35
            reasons.append(f"dossier film: {part}")

    if root_parts:
        for part in root_parts:
            if part in SHOW_HINTS:
                episode_score += 25
                reasons.append(f"root hint série: {part}")
            if part in MOVIE_HINTS:
                movie_score += 25
                reasons.append(f"root hint film: {part}")

    if len(path_parts) >= 2:
        parent = path_parts[-2]
        if _is_season_folder(parent) is not None:
            episode_score += 30
            reasons.append("parent du fichier ressemble à une saison")

    if year is not None:
        movie_score += 20
        reasons.append(f"année détectée: {year}")

    noise_count = _count_noise_words(name_no_ext)
    if year is not None and noise_count >= 2:
        movie_score += 20
        reasons.append("nom ressemble à un release film")

    if year is not None and parsed_ep["episode"] is None:
        movie_score += 10
        reasons.append("année sans pattern épisode")

    if parsed_ep["pattern"] in ("ep_only", "path_ep_only"):
        show_support = 0
        if any(part in SHOW_HINTS for part in path_parts):
            show_support += 1
        if any(_is_season_folder(p) is not None for p in path_parts):
            show_support += 1

        if show_support == 0:
            episode_score -= 20
            reasons.append("ep_only sans support structurel")

    series_title = None
    normalized_series_title = None

    guessed_series_title = _guess_series_title_from_path(raw_path, root=root)
    if guessed_series_title:
        series_title = guessed_series_title
        normalized_series_title = normalize_series_title(guessed_series_title)
        reasons.append(f"series_title depuis path: {guessed_series_title}")

    if parsed_ep["episode"] is not None and not normalized_series_title:
        title_guess = parsed_ep["title_guess"]
        if title_guess:
            normalized_series_title = title_guess
            series_title = title_guess
            reasons.append("series_title fallback depuis filename")

    if episode_score >= movie_score + 15 and episode_score >= 60:
        media_type = "episode"
    elif movie_score >= episode_score + 10 and movie_score >= 30:
        media_type = "movie"
    else:
        media_type = "unknown"

    if media_type == "unknown" and parsed_ep["pattern"] in ("sxe", "nxn", "season_episode"):
        media_type = "episode"
        reasons.append("force episode par pattern fort")

    if media_type == "unknown" and year is not None and any(p in MOVIE_HINTS for p in path_parts):
        media_type = "movie"
        reasons.append("force movie par année+dossier film")

    score_used = max(movie_score, episode_score)

    if score_used >= 180:
        confidence = 99
    elif score_used >= 150:
        confidence = 95
    elif score_used >= 120:
        confidence = 90
    elif score_used >= 90:
        confidence = 80
    elif score_used >= 70:
        confidence = 68
    else:
        confidence = 55

    if media_type == "episode":
        normalized_title = normalize_series_title(name_no_ext)
    else:
        normalized_title = normalize_title(name_no_ext)

    if (
        media_type != "episode"
        and parsed_ep["season"] is not None
        and parsed_ep["episode"] is not None
    ):
        media_type = "episode"
        reasons.append("force episode par saison+episode explicites")        

    return {
        "media_type": media_type,
        "season": season,
        "episode": episode,
        "year": year,
        "normalized_title": normalized_title,
        "series_title": series_title,
        "normalized_series_title": normalized_series_title,
        "scores": {
            "movie": movie_score,
            "episode": episode_score,
        },
        "confidence": confidence,
        "reasons": reasons,
    }


def is_video_file(filename):
    allowed = [".mp4", ".mkv", ".avi", ".m4v", ".ts"]
    ext = os.path.splitext(filename)[1].lower()
    return ext in allowed


_FTP_SCAN_PHASES = {"starting", "scanning", "comparing"}


def _ftp_scan_is_running():
    """Vrai si au moins un scan FTP réel est en cours (pas juste la comparaison dashboard)."""
    # Vérifier le statut global (backward compat)
    if ftp_index_status.get("running") and ftp_index_status.get("phase") in _FTP_SCAN_PHASES:
        return True
    # Vérifier les statuts par serveur
    return any(
        s.get("phase") in _FTP_SCAN_PHASES
        for s in ftp_index_statuses.values()
    )


def _ftp_scan_is_running_for(ftp_id: str) -> bool:
    """Vrai si un scan FTP réel est en cours pour ce serveur spécifique."""
    s = ftp_index_statuses.get(ftp_id, {})
    return s.get("phase") in _FTP_SCAN_PHASES


def ensure_ftp_index(caller="inconnu"):
    """
    S'assure que l'index FTP est prêt pour tous les serveurs activés.
    Retourne (merged_data, state) — rétrocompatibilité avec l'existant.
    state : "ready" | "running" | "built"
    """
    import logging
    log = logging.getLogger(__name__)

    try:
        servers = [s for s in (current_app.config.get("FTP_SERVERS") or [])
                   if s.get("enabled") and s.get("host")]
    except Exception:
        servers = []

    if not servers:
        return {"items": []}, "ready"

    # Si un scan est déjà en cours sur n'importe quel serveur, attendre
    with ftp_index_lock:
        if _ftp_scan_is_running():
            return load_ftp_index() or {"items": []}, "running"

    merged_items = []
    any_built = False
    any_running = False

    for srv in servers:
        ftp_id = srv["id"]
        data, _, _, _ = _get_ftp_index_cached(ftp_id)
        items = data.get("items", []) if data else []

        with ftp_index_lock:
            if _ftp_scan_is_running_for(ftp_id):
                any_running = True
                continue

        if items:
            merged_items.extend(items)
            continue

        # Index vide pour ce serveur → déclencher un scan
        with ftp_index_lock:
            if _ftp_scan_is_running_for(ftp_id):
                any_running = True
                continue
            log.info("[FTP] Scan déclenché par: %s pour '%s' (index vide)", caller, srv.get("name"))
            ftp_index_status.update({
                "running": True, "finished": False, "phase": "starting",
                "current_root": None, "progress": 0, "total": 0,
                "files_found": 0, "estimated_total_files": None, "estimated_percent": 0,
                "comparison_done": 0, "comparison_total": 0, "comparison_percent": 0,
                "message": f"Démarrage du scan FTP (demandé par {caller})...",
            })

        try:
            data = _build_single_ftp_index(srv)
            merged_items.extend(data.get("items", []))
            any_built = True
        except Exception:
            with ftp_index_lock:
                ftp_index_status.update({
                    "running": False, "finished": False, "phase": "idle",
                    "current_root": None, "estimated_percent": None,
                    "comparison_percent": 0, "message": "Erreur index FTP",
                })
            raise

    if any_running:
        return {"items": merged_items}, "running"
    state = "built" if any_built else "ready"
    return {"items": merged_items}, state

def get_ftp_index_file(ftp_id: str = "ftp_1") -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", ftp_id)
    return os.path.join(CACHE_DIR, f"ftp_index_{safe_id}.json")


def get_ftp_refresh_hours(ftp_id: str | None = None) -> int:
    """Retourne le refresh_hours du serveur donné (ou le premier si ftp_id=None)."""
    try:
        servers = current_app.config.get("FTP_SERVERS") or []
        if ftp_id:
            srv = next((s for s in servers if s.get("id") == ftp_id), None)
        else:
            srv = servers[0] if servers else None
        return int((srv or {}).get("refresh_hours", 12))
    except Exception:
        return 12


def load_ftp_index(ftp_id: str | None = None):
    """
    Charge l'index FTP.
    - ftp_id fourni → retourne l'index de ce serveur uniquement.
    - ftp_id=None → fusionne les items de tous les serveurs (rétrocompatibilité).
    """
    if ftp_id is not None:
        path = get_ftp_index_file(ftp_id)
        if not os.path.exists(path):
            return {"generated_at": None, "items": []}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"generated_at": None, "items": []}

    # Fusion de tous les serveurs
    try:
        servers = current_app.config.get("FTP_SERVERS") or []
    except Exception:
        servers = []
    if not servers:
        # Essai de rétrocompatibilité : ancien fichier ftp_index.json
        legacy = os.path.join(CACHE_DIR, "ftp_index.json")
        if os.path.exists(legacy):
            try:
                with open(legacy, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {"generated_at": None, "items": []}

    merged_items = []
    latest_at = None
    for srv in servers:
        d = load_ftp_index(srv["id"])
        merged_items.extend(d.get("items", []))
        if d.get("generated_at"):
            if latest_at is None or d["generated_at"] > latest_at:
                latest_at = d["generated_at"]
    return {"generated_at": latest_at, "items": merged_items}


def save_ftp_index(data, ftp_id: str):
    path = get_ftp_index_file(ftp_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    _invalidate_ftp_memory_cache(ftp_id)


def build_ftp_index(ftp_cfg=None):
    """
    Point d'entrée public.
    - ftp_cfg fourni → rebuild uniquement ce serveur.
    - ftp_cfg=None   → rebuild tous les serveurs activés.
    Retourne les données du/des serveurs scannés (items fusionnés si plusieurs).
    """
    if ftp_cfg is not None:
        return _build_single_ftp_index(ftp_cfg)

    # Rebuild tous les serveurs activés
    try:
        servers = [s for s in (current_app.config.get("FTP_SERVERS") or [])
                   if s.get("enabled") and s.get("host")]
    except Exception:
        servers = []

    if not servers:
        return {"generated_at": datetime.now().isoformat(), "items": []}

    merged_items = []
    for srv in servers:
        data = _build_single_ftp_index(srv)
        merged_items.extend(data.get("items", []))
    return {"generated_at": datetime.now().isoformat(), "items": merged_items}


def _build_single_ftp_index(ftp_cfg: dict):
    """Construit l'index FTP pour un seul serveur FTP."""
    ftp_id   = ftp_cfg["id"]
    existing = load_ftp_index(ftp_id)
    estimated_total = (
        existing.get("scan_stats", {}).get("file_count")
        if existing else None
    )

    valid_roots = [r.strip() for r in (ftp_cfg.get("roots") or []) if str(r).strip()]

    if not ftp_cfg.get("enabled", True):
        with ftp_index_lock:
            ftp_index_status.update({
                "running": False, "phase": "idle", "current_root": None,
                "progress": 0, "total": 0, "files_found": 0,
                "estimated_total_files": None, "estimated_percent": 0,
                "comparison_done": 0, "comparison_total": 0, "comparison_percent": 0,
                "finished": False, "message": f"FTP '{ftp_cfg.get('name')}' désactivé",
            })
            ftp_index_statuses[ftp_id] = dict(ftp_index_status)

        data = {
            "generated_at": datetime.now().isoformat(),
            "scan_stats": {"file_count": 0},
            "items": []
        }
        save_ftp_index(data, ftp_id)
        return data

    host     = (ftp_cfg.get("host") or "").strip()
    port     = int(ftp_cfg.get("port", 21))
    username = (ftp_cfg.get("user") or "").strip()
    password = ftp_cfg.get("pass") or ""
    use_tls  = bool(ftp_cfg.get("tls", False))
    passive  = bool(ftp_cfg.get("passive", True))

    if not host:
        raise ValueError(f"FTP_HOST vide pour '{ftp_cfg.get('name')}'.")
    if not valid_roots:
        raise ValueError(f"FTP_ROOTS vide pour '{ftp_cfg.get('name')}'.")

    import time as _time
    _scan_start = _time.time()

    with ftp_index_lock:
        ftp_index_status.update({
            "running": True,
            "phase": "scanning",
            "current_root": None,
            "progress": 0,
            "total": len(valid_roots),
            "files_found": 0,
            "estimated_total_files": estimated_total,
            "estimated_percent": 0,
            "comparison_done": 0,
            "comparison_total": 0,
            "comparison_percent": 0,
            "finished": False,
            "message": "Construction de l'index FTP...",
            "started_at": _scan_start,
        })

    current_app.logger.info("Construction de l'index FTP...")
    current_app.logger.info("FTP host=%s port=%s roots=%s", host, port, valid_roots)

    all_items = []

    try:
        for i, root in enumerate(valid_roots, start=1):
            # Vérifier annulation avant chaque root
            if ftp_index_status.get("cancel_requested"):
                current_app.logger.info("[FTP] Scan annulé par l'utilisateur avant root %s.", root)
                with ftp_index_lock:
                    ftp_index_status.update({
                        "running": False, "finished": False,
                        "phase": "cancelled", "cancel_requested": False,
                        "message": "Scan FTP annulé.",
                    })
                return {"generated_at": None, "items": [], "cancelled": True}

            ftp = None
            root_items_before = len(all_items)

            with ftp_index_lock:
                ftp_index_status["current_root"] = root
                ftp_index_status["progress"] = i
                ftp_index_status["phase"] = "scanning"
                ftp_index_status["comparison_done"] = 0
                ftp_index_status["comparison_total"] = 0
                ftp_index_status["comparison_percent"] = 0
                ftp_index_status["message"] = f"Scan du root {root}"

            try:
                ftp = get_ftp_client(
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    use_tls=use_tls,
                    passive=passive,
                )

                current_app.logger.info("[FTP] Début scan root: %s", root)

                files = ftp_walk_recursive(
                    ftp=ftp,
                    base_dir=root,
                    is_video_file_func=is_video_file,
                    status_dict=ftp_index_status,
                )

                current_app.logger.info(
                    "[FTP] Fin scan root: %s -> %s fichiers vidéo",
                    root,
                    len(files),
                )

                with ftp_index_lock:
                    ftp_index_status["phase"] = "comparing"
                    ftp_index_status["comparison_done"] = 0
                    ftp_index_status["comparison_total"] = len(files)
                    ftp_index_status["comparison_percent"] = 0
                    ftp_index_status["message"] = f"Analyse du root {root}"

                current_app.logger.debug(
                    "[FTP] Début analyse root: %s -> %s fichiers à normaliser",
                    root,
                    len(files),
                )

                for f in files:
                    # Vérifier annulation pendant l'analyse des fichiers
                    if ftp_index_status.get("cancel_requested"):
                        current_app.logger.info("[FTP] Analyse annulée par l'utilisateur.")
                        with ftp_index_lock:
                            ftp_index_status.update({
                                "running": False, "finished": False,
                                "phase": "cancelled", "cancel_requested": False,
                                "message": "Scan FTP annulé.",
                            })
                        return {"generated_at": None, "items": [], "cancelled": True}

                    name = f.get("name", "")
                    path = f.get("path", "")
                    size = f.get("size", 0)

                    detected = detect_media_type(name=name, path=path, root=root)

                    item = {
                        "name": name,
                        "path": path,
                        "size": size,
                        "ftp_id": ftp_id,
                        "normalized_title": detected["normalized_title"],
                        "year": detected["year"],
                        "media_type": detected["media_type"],
                        "season": detected["season"],
                        "episode": detected["episode"],
                        "root": root,
                        "series_title": detected["series_title"],
                        "normalized_series_title": detected["normalized_series_title"],
                        "detect_scores": detected["scores"],
                        "detect_confidence": detected["confidence"],
                        "detect_reasons": detected["reasons"],
                    }

                    all_items.append(item)

                    with ftp_index_lock:
                        ftp_index_status["comparison_done"] += 1
                        comp_total = ftp_index_status.get("comparison_total", 0)
                        if comp_total > 0:
                            ftp_index_status["comparison_percent"] = int(
                                (ftp_index_status["comparison_done"] / comp_total) * 100
                            )
                        else:
                            ftp_index_status["comparison_percent"] = 0

                    if detected["media_type"] == "episode":
                        current_app.logger.debug(
                            "EPISODE DETECTÉ | conf=%s | show=%s | S%sE%s | file=%s | scores=%s | reasons=%s",
                            detected["confidence"],
                            detected["series_title"],
                            detected["season"],
                            detected["episode"],
                            name,
                            detected["scores"],
                            " | ".join(detected["reasons"]),
                        )
                    elif detected["media_type"] == "movie":
                        current_app.logger.debug(
                            "FILM DETECTÉ | conf=%s | title=%s | year=%s | file=%s | scores=%s | reasons=%s",
                            detected["confidence"],
                            detected["normalized_title"],
                            detected["year"],
                            name,
                            detected["scores"],
                            " | ".join(detected["reasons"]),
                        )
                    else:
                        current_app.logger.debug(
                            "MEDIA UNKNOWN | file=%s | path=%s | scores=%s | reasons=%s",
                            name,
                            path,
                            detected["scores"],
                            " | ".join(detected["reasons"]),
                        )

                current_app.logger.info(
                    "[FTP] Fin analyse root: %s -> %s nouveaux items (%s cumulés)",
                    root,
                    len(all_items) - root_items_before,
                    len(all_items),
                )

            except Exception:
                current_app.logger.exception("[FTP] Erreur root %s", root)

            finally:
                if ftp:
                    try:
                        ftp.quit()
                    except Exception:
                        pass

        current_app.logger.warning("[FTP] TOTAL FILES INDEXÉS: %s", len(all_items))

        data = {
            "generated_at": datetime.now().isoformat(),
            "scan_stats": {
                "file_count": len(all_items)
            },
            "items": all_items
        }

        save_ftp_index(data, ftp_id)

        _scan_duration = round(_time.time() - _scan_start, 1)
        _finished_at = datetime.now().isoformat(timespec="seconds")
        current_app.logger.info(
            "Index FTP construit: %s fichiers en %ss", len(all_items), _scan_duration
        )

        _final_ok = {
            "running": False, "finished": True, "phase": "ready",
            "current_root": None, "estimated_percent": 100,
            "comparison_percent": 100, "message": f"FTP '{ftp_cfg.get('name')}' prêt",
            "last_scan_duration_s": _scan_duration,
            "last_scan_finished_at": _finished_at,
            "started_at": None, "ftp_id": ftp_id,
            "item_count": len(all_items),
        }
        with ftp_index_lock:
            ftp_index_status.update(_final_ok)
            ftp_index_statuses[ftp_id] = dict(_final_ok)

        return data

    except Exception:
        _final_err = {
            "running": False, "finished": False, "phase": "idle",
            "current_root": None, "estimated_percent": None,
            "comparison_percent": 0, "message": "Erreur index FTP",
            "ftp_id": ftp_id,
        }
        with ftp_index_lock:
            ftp_index_status.update(_final_err)
            ftp_index_statuses[ftp_id] = dict(_final_err)
        raise


def ftp_index_is_stale(data, ftp_id: str | None = None):
    generated_at = data.get("generated_at")
    if not generated_at:
        return True
    try:
        dt = datetime.fromisoformat(generated_at)
        return datetime.now() - dt > timedelta(hours=get_ftp_refresh_hours(ftp_id))
    except Exception:
        return True




def confidence_from_score(score, variant_type="full"):
    if score >= 220:
        conf = 99
    elif score >= 180:
        conf = 95
    elif score >= 150:
        conf = 90
    elif score >= 120:
        conf = 82
    elif score >= 95:
        conf = 72
    elif score >= 75:
        conf = 62
    else:
        conf = 50

    if variant_type == "truncated":
        conf -= 8

    return max(0, min(99, conf))


def _get_api_key():
    try:
        from flask import current_app
        return current_app.config.get("TMDB_API_KEY") or ""
    except Exception:
        return ""


def _get_servers_for_search(ftp_ids):
    """Retourne la liste des serveurs FTP à interroger."""
    try:
        all_servers = [s for s in (current_app.config.get("FTP_SERVERS") or []) if s.get("enabled")]
    except Exception:
        return []
    if ftp_ids is None:
        return all_servers
    return [s for s in all_servers if s["id"] in ftp_ids]


def find_ftp_matches_for_movie(title, year=None, ftp_ids=None):
    from app.services.ftp_alias_service import resolve_alias
    ensure_ftp_index(caller="find_ftp_matches_for_movie")

    servers = _get_servers_for_search(ftp_ids)
    movie_items = []
    for srv in servers:
        _, _, _, srv_movies = _get_ftp_index_cached(srv["id"])
        movie_items.extend(srv_movies)

    alias_title = resolve_alias(title, media_type="movie", api_key=_get_api_key())

    wanted_variants = get_title_variants(title)
    if alias_title:
        for v in get_title_variants(alias_title):
            if v not in wanted_variants:
                wanted_variants.append(v)
    matches = []

    for item in movie_items:

        item_title = item.get("normalized_title", "")
        item_tokens = title_tokens(item_title)
        if not item_tokens:
            continue

        best_result = None

        for wanted_norm, variant_type in wanted_variants:
            wanted_tokens = title_tokens(wanted_norm)
            if not wanted_tokens:
                continue

            common = sorted(set(wanted_tokens) & set(item_tokens))
            containment = token_containment(wanted_tokens, item_tokens)
            similarity = token_set_similarity(wanted_tokens, item_tokens)

            score = 0

            if wanted_norm == item_title:
                score += 140 if variant_type == "full" else 95

            if set(wanted_tokens).issubset(set(item_tokens)):
                score += 80 if variant_type == "full" else 45

            score += int(containment * (80 if variant_type == "full" else 45))
            score += int(similarity * (40 if variant_type == "full" else 20))

            item_year = item.get("year")
            if year and item_year:
                diff = abs(int(item_year) - int(year))
                if diff == 0:
                    score += 35
                elif diff == 1:
                    score += 15
                else:
                    score -= 80

            min_common = 1 if len(wanted_tokens) <= 2 else 2
            min_containment = 0.60 if variant_type == "full" else 0.90

            if containment < min_containment:
                continue

            if len(common) < min_common:
                continue

            confidence = confidence_from_score(score, variant_type)

            result = {
                "item": item,
                "score": score,
                "confidence": confidence,
                "variant_type": variant_type,
                "common_tokens": common,
            }

            if best_result is None or result["score"] > best_result["score"]:
                best_result = result

        if best_result:
            matches.append(best_result)

    matches.sort(
        key=lambda x: (
            x["score"],
            x["item"].get("detect_confidence", 0),
            x["item"].get("size", 0)
        ),
        reverse=True
    )

    if matches:
        current_app.logger.debug(
            "FTP movie candidates pour %s: %s",
            title,
            [
                (
                    m["confidence"],
                    m["variant_type"],
                    m["item"].get("name")
                )
                for m in matches[:5]
            ]
        )

    return matches


def find_ftp_match_for_episode(show_title, season, episode, ftp_ids=None):
    if season is None or episode is None:
        return None

    try:
        season = int(season)
        episode = int(episode)
    except (TypeError, ValueError):
        return None

    from app.services.ftp_alias_service import resolve_alias
    ensure_ftp_index(caller="find_ftp_match_for_episode")

    servers = _get_servers_for_search(ftp_ids)
    episode_index: dict = {}
    for srv in servers:
        _, srv_ep_idx, _, _ = _get_ftp_index_cached(srv["id"])
        for key, items in srv_ep_idx.items():
            episode_index.setdefault(key, []).extend(items)

    alias_title = resolve_alias(show_title, media_type="tv", api_key=_get_api_key())
    search_titles = [show_title]
    if alias_title:
        search_titles.append(alias_title)

    wanted_tokens = series_match_tokens(show_title)
    if not wanted_tokens and alias_title:
        wanted_tokens = series_match_tokens(alias_title)

    if not wanted_tokens:
        return None

    # Lookup O(1) par (season, episode) au lieu de parcourir tout l'index
    candidates_items = episode_index.get((season, episode), [])

    candidates = []
    for item in candidates_items:
        item_series = item.get("normalized_series_title") or item.get("series_title") or ""
        if not item_series:
            continue

        best_title_match = None
        for t in search_titles:
            tm = series_title_match_score(t, item_series)
            if best_title_match is None or tm["score"] > best_title_match["score"]:
                best_title_match = tm

        title_match = best_title_match
        score = title_match["score"]
        score += int((item.get("detect_confidence", 0) or 0) / 10)

        containment = title_match["containment"]
        common_tokens = title_match["common_tokens"]

        min_common = 1 if len(wanted_tokens) <= 2 else 2
        min_containment = 0.50 if len(wanted_tokens) <= 2 else 0.70

        if containment < min_containment:
            continue
        if len(common_tokens) < min_common:
            continue

        candidates.append({
            "score": score,
            "item": item,
            "containment": containment,
            "common_tokens": common_tokens,
        })

    candidates.sort(
        key=lambda x: (x["score"], x["item"].get("detect_confidence", 0), x["item"].get("size", 0)),
        reverse=True
    )

    if candidates:
        current_app.logger.debug(
            "FTP episode candidates pour %s S%sE%s: %s",
            show_title, season, episode,
            [(c["score"], c["containment"], c["common_tokens"], c["item"].get("name")) for c in candidates[:5]]
        )

    return candidates[0]["item"] if candidates else None