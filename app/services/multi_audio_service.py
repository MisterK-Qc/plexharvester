"""
Multi Audio Merger — service wrapper.

Fusionne des fichiers MKV en plusieurs langues en un seul fichier multi-audio.
Utilise ffmpeg + alass. Supporte N langues simultanées.
"""

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import threading
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Patterns de détection de langue
# ---------------------------------------------------------------------------

EDITION_PATTERNS: dict[str, list[str]] = {
    "fr": [
        r"\{edition-Fran[çc]ais\}",
        r"\{edition-French\}",
        r"\.fre\.",
        r"\.fra\.",
        r"\.french\.",
        r"\bVFF\b",
        r"\bVF\b",
        r"\bVFQ\b",
        r"\bTRUEFRENCH\b",
        r"\bVOSTFR\b",
        r"\bMULTi\b",
        r"\.vff\.",
        r"\.vf\.",
    ],
    "en": [
        r"\{edition-English\}",
        r"\{edition-Anglais\}",
        r"\.eng\.",
        r"\.english\.",
        r"\bENG\b",
        r"\bENGLISH\b",
    ],
    "de": [
        r"\{edition-German\}",
        r"\{edition-Deutsch\}",
        r"\.ger\.",
        r"\.deu\.",
        r"\.german\.",
    ],
    "es": [
        r"\{edition-Spanish\}",
        r"\{edition-Espagnol\}",
        r"\.spa\.",
        r"\.spanish\.",
    ],
    "it": [
        r"\{edition-Italian\}",
        r"\{edition-Italien\}",
        r"\.ita\.",
        r"\.italian\.",
    ],
    "pt": [
        r"\{edition-Portuguese\}",
        r"\{edition-Portugais\}",
        r"\.por\.",
        r"\.portuguese\.",
    ],
    "ja": [
        r"\{edition-Japanese\}",
        r"\{edition-Japonais\}",
        r"\.jpn\.",
        r"\.japanese\.",
    ],
    "ko": [
        r"\{edition-Korean\}",
        r"\{edition-Cor[eé]en\}",
        r"\.kor\.",
        r"\.korean\.",
    ],
    "zh": [
        r"\{edition-Chinese\}",
        r"\{edition-Chinois\}",
        r"\.zho\.",
        r"\.chi\.",
        r"\.chinese\.",
    ],
}

LANG_CODES: dict[str, str] = {
    "fr": "fre", "en": "eng", "de": "ger", "es": "spa",
    "it": "ita", "pt": "por", "ja": "jpn", "ko": "kor", "zh": "zho",
}

LANG_NAMES: dict[str, str] = {
    "fr": "Français", "en": "English", "de": "Deutsch", "es": "Español",
    "it": "Italiano", "pt": "Português", "ja": "日本語", "ko": "한국어", "zh": "中文",
}

EPISODE_PATTERN = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})", re.IGNORECASE)
VIDEO_EXTENSIONS = {".mkv"}

# Heuristic pour détecter un titre français sans marqueur de langue
_FRENCH_ACCENTED = re.compile(r"[àâæçéèêëîïôœùûü]", re.IGNORECASE)
_FRENCH_WORDS = frozenset([
    "une", "les", "des", "est", "avec", "dans", "pour",
    "sur", "par", "que", "qui", "ses", "son", "aux",
    "leur", "leurs", "nous", "vous", "eux", "cette",
    "tout", "tous", "toute", "plus", "très",
])


def _looks_french(filename: str) -> bool:
    """Heuristic: le nom de fichier ressemble-t-il à un titre français ?
    Vérifie les caractères accentués ou ≥2 mots français communs dans le stem.
    """
    stem = Path(filename).stem
    if _FRENCH_ACCENTED.search(stem):
        return True
    words = set(re.sub(r"[._\-]+", " ", stem).lower().split())
    return sum(1 for w in words if w in _FRENCH_WORDS) >= 2


# ---------------------------------------------------------------------------
# État du job
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_state: dict = {
    "running": False,
    "done": False,
    "log": deque(maxlen=500),
    "success": 0,
    "failed": 0,
    "total": 0,
    "processed": 0,
}


def get_multi_audio_status() -> dict:
    with _lock:
        return {
            "running": _state["running"],
            "done": _state["done"],
            "log": list(_state["log"]),
            "success": _state["success"],
            "failed": _state["failed"],
            "total": _state["total"],
            "processed": _state["processed"],
        }


def _log(msg: str) -> None:
    with _lock:
        _state["log"].append(msg)
    logger.info("[multi-audio] %s", msg)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class VideoInfo:
    path: Path
    width: int = 0
    height: int = 0
    video_bitrate: int = 0
    duration: float = 0.0
    audio_tracks: list = field(default_factory=list)
    subtitle_tracks: list = field(default_factory=list)
    file_size: int = 0

    @property
    def quality_score(self) -> int:
        return self.width * self.height * (self.video_bitrate or 1)


@dataclass
class MediaPair:
    files: dict = field(default_factory=dict)  # {lang_code: VideoInfo}
    match_key: str = ""
    match_type: str = ""

    @property
    def is_complete(self) -> bool:
        return len(self.files) >= 2

    @property
    def reference(self) -> Optional[VideoInfo]:
        if not self.files:
            return None
        return max(self.files.values(), key=lambda v: v.quality_score)

    @property
    def ref_lang(self) -> str:
        ref = self.reference
        return next((lang for lang, v in self.files.items() if v is ref), "")

    @property
    def secondaries(self) -> dict:
        """Dict {lang: VideoInfo} de toutes les pistes non-référence."""
        rl = self.ref_lang
        return {lang: v for lang, v in self.files.items() if lang != rl}


# ---------------------------------------------------------------------------
# ffprobe
# ---------------------------------------------------------------------------

def _run_ffprobe(filepath: Path) -> dict:
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json",
           "-show_format", "-show_streams", str(filepath)]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)
        return json.loads(result.stdout)
    except Exception as e:
        logger.debug("ffprobe failed for %s: %s", filepath, e)
        return {}


def _get_video_info(filepath: Path) -> VideoInfo:
    info = VideoInfo(path=filepath, file_size=filepath.stat().st_size)
    probe = _run_ffprobe(filepath)
    if not probe:
        return info
    fmt = probe.get("format", {})
    info.duration = float(fmt.get("duration", 0))
    for stream in probe.get("streams", []):
        ct = stream.get("codec_type", "")
        if ct == "video":
            info.width = int(stream.get("width", 0))
            info.height = int(stream.get("height", 0))
            br = stream.get("bit_rate")
            if br:
                info.video_bitrate = int(br)
            elif not info.video_bitrate:
                fmt_br = fmt.get("bit_rate")
                if fmt_br:
                    info.video_bitrate = int(fmt_br)
        elif ct == "audio":
            info.audio_tracks.append({
                "index": stream.get("index"),
                "codec": stream.get("codec_name", "unknown"),
                "channels": stream.get("channels", 0),
                "language": stream.get("tags", {}).get("language", "und"),
                "title": stream.get("tags", {}).get("title", ""),
                "bitrate": int(stream.get("bit_rate", 0)),
            })
        elif ct == "subtitle":
            info.subtitle_tracks.append({
                "index": stream.get("index"),
                "codec": stream.get("codec_name", "unknown"),
                "language": stream.get("tags", {}).get("language", "und"),
                "title": stream.get("tags", {}).get("title", ""),
            })
    return info


# ---------------------------------------------------------------------------
# Détection de langue & matching
# ---------------------------------------------------------------------------

def _detect_language(filename: str) -> Optional[str]:
    for lang, patterns in EDITION_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, filename, re.IGNORECASE):
                return lang
    return None


def _extract_episode_key(filename: str) -> Optional[str]:
    match = EPISODE_PATTERN.search(filename)
    if match:
        return f"S{int(match.group(1)):02d}E{int(match.group(2)):02d}"
    return None


def _normalize_title(filename: str) -> str:
    name = Path(filename).stem
    for patterns in EDITION_PATTERNS.values():
        for pattern in patterns:
            name = re.sub(pattern, "", name, flags=re.IGNORECASE)
    tech_patterns = [
        r"\b\d{3,4}p\b", r"\bx26[45]\b", r"\b[Hh]\.?26[45]\b",
        r"\bHEVC\b", r"\bAVC\b", r"\bBlu-?[Rr]ay\b",
        r"\bWEB-?(?:DL|Rip)\b", r"\bRemux\b", r"\bHDR\d*\b",
        r"\bDV\b", r"\bDoVi\b", r"\bDTS(?:-HD)?(?:\.MA)?\b",
        r"\bTrueHD\b", r"\bAtmos\b", r"\bAAC\b", r"\bFLAC\b",
        r"\bAC3\b", r"\bEAC3\b", r"\b5\.1\b", r"\b7\.1\b",
        r"\b\d+bit\b", r"\[\w+\]", r"\(\w+\)",
    ]
    for pattern in tech_patterns:
        name = re.sub(pattern, "", name, flags=re.IGNORECASE)
    name = re.sub(r"[._\-]+", " ", name)
    return re.sub(r"\s+", " ", name).strip().lower()


def _find_pairs(directory: Path, default_lang: str = "en") -> list:
    mkv_files = sorted(
        f for f in directory.iterdir()
        if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS
    )
    if not mkv_files:
        return []

    # Pass 1: group files by episode/title key, recording detected langs and undetected
    detected: dict[str, dict[str, Path]] = {}   # key → {lang: path}
    undetected: dict[str, list[Path]] = {}       # key → [path, ...]
    match_types: dict[str, str] = {}

    for f in mkv_files:
        ep_key = _extract_episode_key(f.name)
        if ep_key:
            key, mt = ep_key, "series"
        else:
            key, mt = _normalize_title(f.name), "movie"
        match_types.setdefault(key, mt)

        lang = _detect_language(f.name)
        if lang is not None:
            detected.setdefault(key, {})[lang] = f
        else:
            undetected.setdefault(key, []).append(f)

    # Pass 2: assign languages to undetected files where possible
    for key, undetected_files in undetected.items():
        detected_langs = detected.get(key, {})

        if len(undetected_files) == 1 and detected_langs and default_lang not in detected_langs:
            # One undetected alongside a detected partner — assign default_lang
            f = undetected_files[0]
            _log(f"ℹ️ Langue non détectée pour '{f.name}' — assignée '{default_lang}' par défaut")
            detected.setdefault(key, {})[default_lang] = f

        elif len(undetected_files) == 1 and detected_langs and default_lang in detected_langs:
            # default_lang already taken — try French heuristic for the undetected file
            f = undetected_files[0]
            if _looks_french(f.name) and "fr" not in detected_langs:
                _log(f"ℹ️ Langue non détectée pour '{f.name}' — assignée 'fr' (titre français)")
                detected.setdefault(key, {})["fr"] = f
            else:
                _log(f"⚠️ Langue non détectée: {f.name}, ignoré")

        elif len(undetected_files) == 2 and not detected_langs:
            # Both files undetected — use French heuristic to distinguish FR from default_lang
            f0, f1 = undetected_files
            f0_fr, f1_fr = _looks_french(f0.name), _looks_french(f1.name)
            if f0_fr ^ f1_fr:
                fr_file, other_file = (f0, f1) if f0_fr else (f1, f0)
                _log(f"ℹ️ Langue non détectée pour '{fr_file.name}' — assignée 'fr' (titre français)")
                _log(f"ℹ️ Langue non détectée pour '{other_file.name}' — assignée '{default_lang}' par défaut")
                detected.setdefault(key, {})["fr"] = fr_file
                detected.setdefault(key, {})[default_lang] = other_file
            else:
                for f in undetected_files:
                    _log(f"⚠️ Langue non détectée: {f.name}, ignoré")

        else:
            for f in undetected_files:
                _log(f"⚠️ Langue non détectée: {f.name}, ignoré")

    # Pass 3: build MediaPair objects
    pairs_dict: dict[str, MediaPair] = {}
    for key, lang_map in detected.items():
        pair = MediaPair(match_key=key, match_type=match_types.get(key, "movie"))
        for lang, path in lang_map.items():
            info = _get_video_info(path)
            if lang in pair.files:
                _log(f"⚠️ Doublon langue {lang.upper()} pour '{key}', ignoré: {path.name}")
                continue
            pair.files[lang] = info
        pairs_dict[key] = pair

    complete = []
    for key, pair in sorted(pairs_dict.items()):
        if pair.is_complete:
            complete.append(pair)
        elif pair.files:
            langs = [lang.upper() for lang in pair.files]
            _log(f"⚠️ '{key}': seulement {langs} trouvé(s), ignoré")
    return complete


# ---------------------------------------------------------------------------
# TMDB original-language lookup
# ---------------------------------------------------------------------------

def _extract_search_title(d: Path) -> str:
    """Extract a searchable title from a directory for TMDB lookup."""
    try:
        for f in sorted(d.iterdir()):
            if f.suffix.lower() == ".mkv":
                m = EPISODE_PATTERN.search(f.name)
                if m:
                    title = f.name[:m.start()].strip(" ._-")
                    title = re.sub(r"[._\-]+", " ", title).strip()
                    if title:
                        return title
    except OSError:
        pass
    name = d.name
    if re.search(r"\bSeason\s*\d+\b|\bS\d{1,2}$", name, re.IGNORECASE):
        name = d.parent.name
    name = re.sub(r"\(\d{4}\)", "", name)
    name = re.sub(r"\b(19|20)\d{2}\b", "", name)
    name = re.sub(r"[._\-]+", " ", name).strip()
    return name


def _tmdb_original_language(title: str, api_key: str) -> Optional[str]:
    """Query TMDB multi-search and return the original_language of the best result."""
    if not api_key or not title:
        return None
    try:
        import urllib.request
        import urllib.parse
        params = urllib.parse.urlencode({
            "api_key": api_key,
            "query": title,
            "language": "en-US",
        })
        url = f"https://api.themoviedb.org/3/search/multi?{params}"
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        results = [r for r in data.get("results", [])
                   if r.get("media_type") in ("movie", "tv")]
        if results:
            lang = results[0].get("original_language", "")
            if lang and 2 <= len(lang) <= 3:
                return lang
    except Exception as e:
        logger.debug("[multi-audio] TMDB lookup '%s': %s", title, e)
    return None


# ---------------------------------------------------------------------------
# Audio sync & muxing
# ---------------------------------------------------------------------------

_ENVELOPE_RATE = 20.0   # frames/sec → 50ms résolution (8000 / 20 = 400 samples)
_SPEECH_BAND_AF = "highpass=f=200,lowpass=f=3500,aresample=8000"  # filtre voix pour xcorr sur films atmosphériques


def _parse_timecode(s: str) -> float:
    """Parse 'HH:MM:SS.mmm', 'MM:SS.mmm' or plain seconds to float seconds."""
    s = s.strip()
    parts = s.split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        if len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
        return float(s)
    except (ValueError, IndexError):
        return float(s)


def _compute_energy_envelope(path: Path, start_sec: float, duration_sec: float,
                              af_filter: str = ""):
    """Extrait l'enveloppe d'énergie RMS (50ms/fenêtre → 20 valeurs/sec).

    Indépendante de la langue : les patterns loudness/silence sont identiques
    entre les versions FR et EN d'un même épisode.
    af_filter : chaîne ffmpeg -af optionnelle (ex: _SPEECH_BAND_AF pour filtrer sur les voix).
    """
    cmd = [
        "ffmpeg", "-y",
        "-ss", f"{start_sec:.3f}", "-t", f"{duration_sec:.3f}",
        "-i", str(path),
        "-ac", "1",
    ]
    if af_filter:
        cmd.extend(["-af", af_filter])
    else:
        cmd.extend(["-ar", "8000"])
    cmd.extend(["-f", "s16le", "pipe:1"])
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=60)
        if result.returncode != 0 or not result.stdout:
            return None
        import numpy as np
        pcm = np.frombuffer(result.stdout, dtype=np.int16).astype(np.float32)
        w = int(8000 / _ENVELOPE_RATE)  # 400 samples = 50ms @ 8kHz
        n_win = len(pcm) // w
        if n_win < 50:
            return None
        return np.sqrt(np.mean(pcm[:n_win * w].reshape(n_win, w) ** 2, axis=1))
    except Exception as exc:
        logger.debug("Energy envelope failed (%s): %s", path.name, exc)
        return None


def _compute_vad_signal(path: Path, start_sec: float, duration_sec: float):
    """Binary VAD signal: 1=speech-like, 0=silence/music.

    Thresholds the speech-band energy envelope at the 30th percentile so that
    the top 70 % of frames are marked as speech.  Correlating binary patterns
    is robust when FR and EN have different music/effects (dubbed series,
    different broadcast packages) since only the rhythm of speech matters.
    """
    env = _compute_energy_envelope(path, start_sec, duration_sec, af_filter=_SPEECH_BAND_AF)
    if env is None:
        return None
    import numpy as np
    threshold = np.percentile(env, 30)
    return (env > threshold).astype(np.float32)


def _xcorr_envelope(ref_env, sec_env, max_search_sec: float) -> tuple[float, float]:
    """Cross-corrélation normalisée sur enveloppes d'énergie.

    Retourne (offset_sec, peak_strength) :
      - offset_sec  : positif = sec en avance sur ref. 0.0 si hors ±max_search_sec.
      - peak_strength : max |corr| normalisé [0..1] — indicateur de confiance.

    Les enveloppes sont centrées (demean) avant la corrélation pour éviter que la
    composante DC (toujours positive sur le RMS) domine le lag 0 et masque les vrais pics.
    """
    import numpy as np
    ENVELOPE_RATE = _ENVELOPE_RATE
    # Centrer pour supprimer la dominance DC (bug classique sur signaux toujours positifs)
    ref_c = ref_env - np.mean(ref_env)
    sec_c = sec_env - np.mean(sec_env)
    ref_n = ref_c / (np.max(np.abs(ref_c)) + 1e-8)
    sec_n = sec_c / (np.max(np.abs(sec_c)) + 1e-8)
    n = len(ref_n) + len(sec_n) - 1
    fft_size = 1 << (n - 1).bit_length()
    R = np.fft.rfft(ref_n, fft_size) * np.conj(np.fft.rfft(sec_n, fft_size))
    corr = np.fft.irfft(R)
    peak = int(np.argmax(np.abs(corr)))
    if peak > fft_size // 2:
        peak -= fft_size
    offset = peak / ENVELOPE_RATE
    peak_strength = float(np.abs(corr[peak]) / (np.sum(corr ** 2) ** 0.5 + 1e-12))
    if abs(offset) > max_search_sec:
        return 0.0, peak_strength
    return offset, peak_strength


def _extract_merged_track_envelope(
    path: Path, audio_track_idx: int, start_sec: float, duration_sec: float,
    af_filter: str = "",
):
    """Extrait l'enveloppe d'énergie d'une piste audio spécifique du fichier fusionné.

    af_filter : chaîne ffmpeg -af complète (ex: "highpass=f=120,lowpass=f=500,aresample=8000").
    Utile pour isoler une bande partagée entre pistes EN/FR et exclure les voix et le LFE.
    """
    cmd = [
        "ffmpeg", "-y",
        "-ss", f"{start_sec:.3f}", "-t", f"{duration_sec:.3f}",
        "-i", str(path),
        "-map", f"0:a:{audio_track_idx}",
        "-ac", "1",
    ]
    if af_filter:
        cmd.extend(["-af", af_filter])
    else:
        cmd.extend(["-ar", "8000"])
    cmd.extend(["-f", "s16le", "pipe:1"])
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=120)
        if result.returncode != 0 or not result.stdout:
            return None
        import numpy as np
        pcm = np.frombuffer(result.stdout, dtype=np.int16).astype(np.float32)
        w = int(8000 / _ENVELOPE_RATE)
        n_win = len(pcm) // w
        if n_win < 50:
            return None
        return np.sqrt(np.mean(pcm[:n_win * w].reshape(n_win, w) ** 2, axis=1))
    except Exception as exc:
        logger.debug("Merged track envelope failed (track %d): %s", audio_track_idx, exc)
        return None


def _analyze_audio_sync(ref_path: Path, sec_path: Path,
                         ref_duration: float, sec_duration: float) -> tuple[float, float]:
    """Détecte l'offset initial et le facteur de vitesse entre deux fichiers.

    Étape 1 — offset initial : cross-corrélation sur le début des fichiers.
               offset > 0 = sec en avance → retarder de offset s (--sync +N ms)
               offset < 0 = sec en retard  → avancer de |offset| s (--sync -N ms)

    Étape 2 — facteur de vitesse :
      • Grande dérive (>0.5%, ex. PAL/NTSC) : ratio des durées totales.
      • Petite dérive (<0.5%) : deux corrélations (début + milieu) pour mesurer
        directement la dérive accumulée → speed = 1 - drift/mid_t.
        Évite de confondre la différence d'intro avec une vraie dérive de vitesse.

    Retourne (0.0, 1.0) si numpy est absent ou si l'analyse échoue.
    """
    try:
        import numpy as np
    except ImportError:
        logger.warning("numpy non disponible — sync audio désactivé")
        return 0.0, 1.0

    SEARCH_SEC = 30.0
    DRIFT_SEARCH = 30.0   # élargi : à 70% du fichier la dérive accumulée peut dépasser 20s
    WINDOW_SEC = 120.0

    # ── Étape 1 : offset au début du fichier ─────────────────────────────────
    ref_env0 = _compute_energy_envelope(ref_path, 0, WINDOW_SEC)
    sec_env0 = _compute_energy_envelope(sec_path, 0, WINDOW_SEC + SEARCH_SEC * 2)

    if ref_env0 is None or sec_env0 is None or len(ref_env0) < 50 or len(sec_env0) < 50:
        logger.debug("Enveloppe début indisponible — offset=0, vitesse=1.0")
        return 0.0, 1.0

    offset_0, _ = _xcorr_envelope(ref_env0, sec_env0, SEARCH_SEC)

    # ── Étape 2a : grande dérive → ratio de durées (PAL/NTSC, >0.5%) ────────
    if ref_duration > 0 and sec_duration > 0:
        d_speed = sec_duration / ref_duration
        if abs(d_speed - 1.0) > 0.005:
            if 0.90 <= d_speed <= 1.10:
                return offset_0, d_speed
            logger.warning("Facteur de vitesse suspect (%.4f) — ignoré", d_speed)
            return offset_0, 1.0

    # ── Étape 2b : petite dérive → corrélation à 70% du fichier ────────────
    mid_t = min(ref_duration * 0.70, 70 * 60.0)
    sec_mid = mid_t - offset_0

    if (mid_t < WINDOW_SEC * 2
            or mid_t + WINDOW_SEC > ref_duration
            or sec_mid < 0
            or sec_mid + WINDOW_SEC + DRIFT_SEARCH * 2 > sec_duration):
        return offset_0, 1.0

    ref_env_mid = _compute_energy_envelope(ref_path, mid_t, WINDOW_SEC)
    sec_env_mid = _compute_energy_envelope(sec_path, sec_mid, WINDOW_SEC + DRIFT_SEARCH * 2)

    if ref_env_mid is None or sec_env_mid is None or len(ref_env_mid) < 50 or len(sec_env_mid) < 50:
        logger.debug("Enveloppe milieu indisponible — vitesse=1.0")
        return offset_0, 1.0

    # drift > 0 : sec plus en avance qu'attendu → ralentir (speed < 1)
    # drift < 0 : sec moins en avance = en retard → accélérer (speed > 1)
    drift, _ = _xcorr_envelope(ref_env_mid, sec_env_mid, DRIFT_SEARCH)
    speed = 1.0 - drift / mid_t
    logger.debug("Dérive mesurée à %.0fmin : %+.2fs → vitesse %.6f", mid_t / 60, drift, speed)

    if not (0.90 <= speed <= 1.10):
        logger.warning("Vitesse dérivée suspecte (%.4f) — ignorée", speed)
        return offset_0, 1.0

    return offset_0, speed


def _robust_linear_fit(
    points: list[tuple[float, float]],
    weights: Optional[list[float]] = None,
) -> tuple[float, float]:
    """Theil-Sen robust estimator: median pairwise slopes + median intercept.

    Robust to ~50 % outlier points (spurious xcorr peaks).
    Returns (slope, intercept) such that y ≈ slope * x + intercept.
    """
    import numpy as np
    n = len(points)
    if n < 2:
        return 1.0, 0.0
    xs = np.array([p[0] for p in points], dtype=np.float64)
    ys = np.array([p[1] for p in points], dtype=np.float64)
    slopes = []
    for i in range(n):
        for j in range(i + 1, n):
            dx = xs[j] - xs[i]
            if abs(dx) > 0.5:
                slopes.append(float((ys[j] - ys[i]) / dx))
    if not slopes:
        return 1.0, 0.0
    slope = float(np.median(slopes))
    intercept = float(np.median(ys - slope * xs))
    return slope, intercept


def _analyze_multi_segment_sync(
    ref_path: Path, sec_path: Path,
    ref_duration: float, sec_duration: float,
    hint_offset_ms: float = 0.0,
    hint_atempo: float = 0.0,
) -> tuple[float, list]:
    """Multi-point xcorr: detect variable-speed drift automatically.

    Probes at N equally-spaced points (15%–85% of file) and computes per-segment
    atempo values.  For each probe at EN time t_en:
        t_fr_actual = (t_en - offset_0) - drift
    where drift < 0 when FR has run faster than expected.

    hint_offset_ms : known initial offset (ms) measured externally (e.g. Audition).
        When provided, the initial xcorr is skipped and this value is used directly as
        offset_0.  Probes still run with the correctly-positioned sec_center, which
        dramatically improves confidence on dubbed content with different intros/outros.

    Returns (offset_sec, segments) where segments is ready for _apply_segmented_correction.
    Returns (offset, []) on analysis failure — caller should fall back to constant speed.
    """
    try:
        import numpy as np  # noqa: F401
    except ImportError:
        return 0.0, []

    SEARCH_SEC  = 30.0
    DRIFT_SEARCH = 45.0
    WINDOW_SEC  = 90.0
    # ~1 probe per 10 min, min 3, max 8
    n_probes = max(3, min(8, int(ref_duration / 600)))

    # ── Step 1: initial offset ────────────────────────────────────────────────
    if hint_offset_ms != 0.0:
        # User-supplied offset: skip xcorr entirely and use it as-is.
        # Set conf0=1.0 so tier initial-offset xcorrs don't accidentally override it
        # (they're unreliable when intro/outro content differs between files).
        offset_0 = hint_offset_ms / 1000.0
        conf0 = 1.0
        _log(f"  Offset initial: {offset_0:+.3f}s (fourni manuellement)")
        # If atempo is also known, skip all xcorr and return a single segment directly.
        if hint_atempo != 0.0:
            _log(f"  Vitesse: {hint_atempo:.5f}x (fournie manuellement) — xcorr ignoré")
            drift_pct = abs(hint_atempo - 1.0) * 100
            _log(f"  1 segment(s) détecté(s):")
            _log(f"    Seg 1: FR 0→fin | atempo={hint_atempo:.5f}x ({drift_pct:.3f}%)")
            return offset_0, [{"fr_start": 0.0, "fr_end": None, "atempo": hint_atempo}]
    else:
        ref_env0 = _compute_energy_envelope(ref_path, 0, WINDOW_SEC)
        sec_env0 = _compute_energy_envelope(sec_path, 0, WINDOW_SEC + SEARCH_SEC * 2)
        if ref_env0 is None or sec_env0 is None or len(ref_env0) < 50 or len(sec_env0) < 50:
            _log("  ⚠️ Multi-seg: enveloppe début indisponible")
            return 0.0, []

        offset_0, conf0 = _xcorr_envelope(ref_env0, sec_env0, SEARCH_SEC)
        _log(f"  Offset initial: {offset_0:+.3f}s (conf={conf0:.3f})")

    # ── Step 2: probes at n_probes equally-spaced points (15%–85%) ───────────
    probe_times = [
        ref_duration * (0.15 + 0.78 * i / max(n_probes - 1, 1))
        for i in range(n_probes)
    ]

    MIN_CONF = 0.10  # probes below this are likely false peaks — discarded

    # Ratio durée sec/ref : corrige la position des probes quand la vitesse diverge
    # significativement (ex: 3.3%). Sans ça, à 28min la position EN est décalée de
    # 55s, ce qui dépasse DRIFT_SEARCH=45s et rend l'xcorr impossible.
    speed_ratio = sec_duration / ref_duration if ref_duration > 0 else 1.0

    # All probe results across every tier — used for regression fallback when dubbed
    # content causes initial xcorr failure (different network intros, recaps, cold opens).
    all_candidates: list[tuple[float, float, float]] = []

    def _run_probes(
        af: str = "", vad: bool = False, min_conf: float = MIN_CONF,
    ) -> tuple[list[tuple[float, float, float]], list[tuple[float, float, float]]]:
        """Run all probe positions — full-spectrum, speech-band, low-freq, or binary VAD.

        Returns (passing, all_in_tier):
          passing     — probes at or above min_conf
          all_in_tier — every probe including low-confidence, for regression fallback
        """
        if vad:
            tag = " [VAD]"
        elif af and "lowpass=f=200" in af:
            tag = " [basses]"
        elif af:
            tag = " [voix]"
        else:
            tag = ""
        found: list[tuple[float, float, float]] = []
        tier_cands: list[tuple[float, float, float]] = []
        for t_en in probe_times:
            sec_center = t_en * speed_ratio - offset_0
            if (t_en + WINDOW_SEC > ref_duration
                    or sec_center < 0
                    or sec_center + WINDOW_SEC + DRIFT_SEARCH * 2 > sec_duration):
                continue
            if vad:
                ref_env = _compute_vad_signal(ref_path, t_en, WINDOW_SEC)
                sec_env = _compute_vad_signal(sec_path, sec_center,
                                              WINDOW_SEC + DRIFT_SEARCH * 2)
            else:
                ref_env = _compute_energy_envelope(ref_path, t_en, WINDOW_SEC, af_filter=af)
                sec_env = _compute_energy_envelope(sec_path, sec_center,
                                                   WINDOW_SEC + DRIFT_SEARCH * 2, af_filter=af)
            if ref_env is None or sec_env is None or len(ref_env) < 50 or len(sec_env) < 50:
                continue
            mins = int(t_en // 60)
            if not vad:
                ref_cv = float(np.std(ref_env) / (np.mean(ref_env) + 1e-8))
                if ref_cv < 0.12:
                    _log(f"  Probe @{mins}min{tag}: ignoré (signal uniforme CV={ref_cv:.3f})")
                    continue
            drift, conf = _xcorr_envelope(ref_env, sec_env, DRIFT_SEARCH)
            t_fr = sec_center - drift
            tier_cands.append((t_en, t_fr, conf))
            if conf < min_conf:
                _log(f"  Probe @{mins}min{tag}: ignoré (conf={conf:.3f} < {min_conf})")
                continue
            found.append((t_en, t_fr, conf))
            _log(f"  Probe @{mins}min{tag}: FR src={t_fr:.1f}s drift={drift:+.3f}s conf={conf:.3f}")
        return found, tier_cands

    probes, _cands = _run_probes()
    all_candidates.extend(_cands)

    # Tier 2: speech-band filter — films with atmospheric audio/score
    if len(probes) < max(2, n_probes // 2):
        _log(f"  Peu de probes valides ({len(probes)}/{n_probes})"
             f" — relance avec filtre voix (200–3500 Hz)...")
        ref_env0_sb = _compute_energy_envelope(ref_path, 0, WINDOW_SEC,
                                                af_filter=_SPEECH_BAND_AF)
        sec_env0_sb = _compute_energy_envelope(sec_path, 0, WINDOW_SEC + SEARCH_SEC * 2,
                                                af_filter=_SPEECH_BAND_AF)
        if (ref_env0_sb is not None and sec_env0_sb is not None
                and len(ref_env0_sb) >= 50 and len(sec_env0_sb) >= 50):
            off_sb, conf_sb = _xcorr_envelope(ref_env0_sb, sec_env0_sb, SEARCH_SEC)
            if conf_sb > conf0:
                offset_0 = off_sb
                _log(f"  Offset initial [voix]: {offset_0:+.3f}s (conf={conf_sb:.3f})")
        probes_sb, _cands = _run_probes(af=_SPEECH_BAND_AF)
        all_candidates.extend(_cands)
        if probes_sb:
            probes = probes_sb

    # Tier 3: low-frequency band (40–200 Hz) — music bass lines and effects are
    # identical between the original and dub; only dialogue has been replaced.
    _LOW_FREQ_AF = "highpass=f=40,lowpass=f=200,aresample=8000"
    if len(probes) < max(2, n_probes // 2):
        _log(f"  Peu de probes valides ({len(probes)}/{n_probes})"
             f" — relance basses fréquences (40–200 Hz)...")
        ref_env0_lf = _compute_energy_envelope(ref_path, 0, WINDOW_SEC,
                                                af_filter=_LOW_FREQ_AF)
        sec_env0_lf = _compute_energy_envelope(sec_path, 0, WINDOW_SEC + SEARCH_SEC * 2,
                                                af_filter=_LOW_FREQ_AF)
        if (ref_env0_lf is not None and sec_env0_lf is not None
                and len(ref_env0_lf) >= 50 and len(sec_env0_lf) >= 50):
            off_lf, conf_lf = _xcorr_envelope(ref_env0_lf, sec_env0_lf, SEARCH_SEC)
            if conf_lf > conf0:
                offset_0 = off_lf
                _log(f"  Offset initial [basses]: {offset_0:+.3f}s (conf={conf_lf:.3f})")
        probes_lf, _cands = _run_probes(af=_LOW_FREQ_AF)
        all_candidates.extend(_cands)
        if probes_lf:
            probes = probes_lf

    # Tier 4: binary VAD signal — dubbed content where FR/EN have different music/effects.
    # Correlates the *rhythm* of speech rather than its energy level.
    # Uses ±60 s initial search to handle series with different intro/recap lengths.
    # MIN_CONF lowered to 0.07 — VAD on dubbed content yields weaker but valid peaks.
    if len(probes) < max(2, n_probes // 2):
        _log(f"  Relance avec signal VAD (contenu doublé, recherche initiale ±60s)...")
        VAD_SEARCH = 60.0
        ref_vad0 = _compute_vad_signal(ref_path, 0, WINDOW_SEC)
        sec_vad0 = _compute_vad_signal(sec_path, 0, WINDOW_SEC + VAD_SEARCH * 2)
        if (ref_vad0 is not None and sec_vad0 is not None
                and len(ref_vad0) >= 50 and len(sec_vad0) >= 50):
            off_vad, conf_vad = _xcorr_envelope(ref_vad0, sec_vad0, VAD_SEARCH)
            if conf_vad > conf0:
                offset_0 = off_vad
                _log(f"  Offset initial [VAD]: {offset_0:+.3f}s (conf={conf_vad:.3f})")
        probes_vad, _cands = _run_probes(vad=True, min_conf=0.07)
        all_candidates.extend(_cands)
        if probes_vad:
            probes = probes_vad

    # Hint refinement: when an offset hint was provided and probes passed with good
    # confidence, refine offset_0 via linear regression on those probes.
    # The hint positions probes correctly; the regression then extracts the exact offset
    # from the measured (t_ref, t_sec) pairs — correcting a hint that's off by tens to
    # hundreds of ms without any extra xcorr passes.
    if hint_offset_ms != 0.0 and len(probes) >= max(2, n_probes // 2):
        pts_r = [(t, f) for t, f, _ in probes]
        ws_r  = [c for _, _, c in probes]
        slope_r, intercept_r = _robust_linear_fit(pts_r, ws_r)
        if 0.90 <= slope_r <= 1.10:
            refined = -intercept_r / slope_r if abs(slope_r) > 0.01 else 0.0
            # Sanity: refuse correction that's more than 30 s away from the hint
            if abs(refined - offset_0) <= 30.0:
                delta = refined - offset_0
                _log(f"  Offset raffiné (régression {len(probes)} probes): "
                     f"{refined:+.3f}s (Δ={delta:+.3f}s vs hint, pente={slope_r:.5f})")
                offset_0 = refined

    # Regression fallback: infer offset_0 from accumulated low-confidence candidates.
    # Handles dubbed series where the first 90 s has completely different content
    # (network cards, recaps, cold opens in different languages) so all xcorr tiers
    # fail for the initial offset. Multiple mid-episode probes with conf < threshold
    # but consistent drift patterns can still triangulate the correct offset via a
    # robust median-slope linear fit (Theil-Sen estimator).
    # Triggered only when fewer probes than needed passed the confidence threshold.
    _reg_slope: Optional[float] = None
    _reg_intercept: Optional[float] = None
    if len(probes) < max(2, n_probes // 2) and len(all_candidates) >= 3:
        import numpy as np  # noqa: F401
        MIN_CAND_CONF = 0.035
        cand_by_t: dict[float, tuple[float, float, float]] = {}
        for t, f, c in all_candidates:
            if c >= MIN_CAND_CONF and (t not in cand_by_t or c > cand_by_t[t][2]):
                cand_by_t[t] = (t, f, c)
        unique_cands = list(cand_by_t.values())
        if len(unique_cands) >= 3:
            pts = [(t, f) for t, f, _ in unique_cands]
            ws = [c for _, _, c in unique_cands]
            slope, intercept = _robust_linear_fit(pts, ws)
            if 0.90 <= slope <= 1.10:
                reg_offset = -intercept / slope if abs(slope) > 0.01 else 0.0
                if -120.0 <= reg_offset <= 120.0:
                    avg_conf = sum(ws) / len(ws)
                    _reg_slope = slope
                    _reg_intercept = intercept
                    if hint_offset_ms != 0.0:
                        # Hint provided — trust it for offset; only take the slope
                        # (speed estimate) from the regression.  Overwriting offset_0
                        # here would defeat the purpose of the hint.
                        _log(f"  Régression sur {len(unique_cands)} candidats: "
                             f"pente={slope:.5f} (conf moy={avg_conf:.3f})"
                             f" — offset conservé depuis hint ({offset_0:+.3f}s)")
                    else:
                        _log(f"  Régression sur {len(unique_cands)} candidats: "
                             f"pente={slope:.5f}, offset inféré={reg_offset:+.3f}s "
                             f"(conf moy={avg_conf:.3f})")
                        offset_0 = reg_offset

    # Prune passing probes that deviate too far from the regression line.
    # A large deviation means xcorr latched onto a spurious peak — using it as a
    # segment boundary would corrupt the atempo (e.g. a VAD probe just above 0.07
    # but 25 s off anchors a single-segment atempo that drifts throughout the episode).
    if _reg_slope is not None and probes:
        REG_OUTLIER_SEC = 20.0
        pruned: list[tuple[float, float, float]] = []
        for t, f, c in probes:
            expected = _reg_slope * t + _reg_intercept
            if abs(f - expected) <= REG_OUTLIER_SEC:
                pruned.append((t, f, c))
            else:
                _log(f"  ⚠️ Probe @{int(t // 60)}min écarté après régression "
                     f"(t_fr={f:.1f}s vs attendu {expected:.1f}s, "
                     f"Δ={abs(f - expected):.1f}s > {REG_OUTLIER_SEC:.0f}s)")
        probes = pruned

    if not probes:
        if _reg_slope is not None:
            # No valid probes remain but regression succeeded — synthesize a probe at
            # mid-episode so segment boundaries use the regression-derived speed.
            mid_t = ref_duration * 0.5
            mid_f = _reg_slope * (mid_t - offset_0)
            _log(f"  Probe synthétique (régression): @{int(mid_t // 60)}min"
                 f" → t_fr={mid_f:.1f}s | atempo≈{_reg_slope:.5f}x")
            probes = [(mid_t, mid_f, 0.0)]
        elif hint_offset_ms != 0.0:
            # Hint provided but no regression data — use file duration ratio as speed
            mid_t = ref_duration * 0.5
            mid_f = speed_ratio * (mid_t - offset_0)
            _log(f"  Probe synthétique (hint + ratio durées): @{int(mid_t // 60)}min"
                 f" → t_fr={mid_f:.1f}s | atempo≈{speed_ratio:.5f}x")
            probes = [(mid_t, mid_f, 0.0)]
        else:
            _log("  ⚠️ Multi-seg: aucun probe valide (spectre complet, voix, basses, VAD)")
            return offset_0, []

    # ── Step 3: build segments from consecutive boundaries ────────────────────
    # boundaries: [(en_time, fr_src_time), ...]
    boundaries = [(offset_0, 0.0)] + [(t_en, t_fr) for t_en, t_fr, _ in probes]

    segments: list[dict] = []
    for i in range(len(boundaries) - 1):
        en_start, fr_start = boundaries[i]
        en_end,   fr_end   = boundaries[i + 1]
        en_dur = en_end - en_start
        fr_dur = fr_end - fr_start
        if en_dur <= 0:
            continue
        atempo = fr_dur / en_dur
        if not (0.90 <= atempo <= 1.10):
            _log(f"  ⚠️ Multi-seg: atempo={atempo:.4f} suspect (seg {i+1}), fallback")
            return offset_0, []
        segments.append({"fr_start": fr_start, "fr_end": fr_end, "atempo": atempo})

    # Extend last segment to EOF with same speed
    if segments:
        segments.append({
            "fr_start": segments[-1]["fr_end"],
            "fr_end":   None,
            "atempo":   segments[-1]["atempo"],
        })

    # Log segment summary
    _log(f"  {len(segments)} segment(s) détecté(s):")
    for j, seg in enumerate(segments):
        fr_end_str = f"{seg['fr_end']:.0f}s" if seg["fr_end"] is not None else "fin"
        drift_pct  = abs(seg["atempo"] - 1.0) * 100
        _log(f"    Seg {j+1}: FR {seg['fr_start']:.0f}→{fr_end_str}"
             f" | atempo={seg['atempo']:.5f}x ({drift_pct:.3f}%)")

    return offset_0, segments


def _compute_segments(sync_points: list, prev_offset_sec: float, prev_speed: float) -> list:
    """Compute FR source segments from Audition sync-point measurements.

    sync_points : [{'en': float_sec, 'fr': float_sec}, ...]
                  Timestamps in the merged output file (EN=reference track, FR=secondary track).
    prev_offset_sec : delay applied to FR in the previous merge (mkvmerge --sync value / 1000).
    prev_speed      : atempo factor applied to FR source in the previous merge.

    Returns list of dicts:
        {'fr_start': float, 'fr_end': float|None, 'atempo': float}
    The last segment always has fr_end=None (runs to EOF).
    """
    # Convert FR merged positions → FR source positions
    #   FR_source = (FR_merged_time - prev_offset_sec) * prev_speed
    boundaries = [(prev_offset_sec, 0.0)]  # implied start: at EN=offset, FR source=0
    for pt in sync_points:
        fr_src = (pt["fr"] - prev_offset_sec) * prev_speed
        boundaries.append((pt["en"], fr_src))

    segments = []
    for i in range(len(boundaries) - 1):
        en_start, fr_start = boundaries[i]
        en_end, fr_end = boundaries[i + 1]
        en_dur = en_end - en_start
        fr_dur = fr_end - fr_start
        if en_dur <= 0:
            logger.warning("Segment %d: en_dur=%.3fs <= 0, ignoré", i, en_dur)
            continue
        atempo = fr_dur / en_dur
        segments.append({"fr_start": fr_start, "fr_end": fr_end, "atempo": atempo})

    # Extend last segment to end of file with the same speed
    if segments:
        last_atempo = segments[-1]["atempo"]
        last_fr_start = segments[-1]["fr_end"]
        segments.append({"fr_start": last_fr_start, "fr_end": None, "atempo": last_atempo})

    return segments


def _apply_segmented_correction(source_path: Path, segments: list,
                                 output_path: Path, tmp_dir: Path, lang: str) -> bool:
    """Extract + atempo each FR source segment, then concat to output_path."""

    # When all segments share the same speed, a single ffmpeg pass is simpler,
    # faster, and avoids multi-channel AAC re-encoding issues during concat.
    first_atempo = segments[0]["atempo"]
    if all(abs(s["atempo"] - first_atempo) < 1e-5 for s in segments):
        _log(f"  ({lang.upper()}) Vitesse uniforme {first_atempo:.5f}x — passe unique")
        cmd = ["ffmpeg", "-y"]
        if segments[0]["fr_start"] > 0.01:
            cmd.extend(["-ss", f"{segments[0]['fr_start']:.3f}"])
        cmd.extend([
            "-i", str(source_path), "-vn",
            "-af", f"atempo={first_atempo:.6f}",
            "-c:a", "aac", "-b:a", "192k",
            str(output_path),
        ])
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode != 0:
                _log(f"  ❌ ({lang.upper()}) ffmpeg: {result.stderr[-300:]}")
                return False
            return True
        except subprocess.TimeoutExpired:
            _log(f"  ❌ ({lang.upper()}) timeout")
            return False

    seg_files = []
    for i, seg in enumerate(segments):
        seg_path = tmp_dir / f"seg_{lang}_{i}.mka"
        cmd = ["ffmpeg", "-y", "-ss", f"{seg['fr_start']:.3f}"]
        if seg["fr_end"] is not None:
            cmd.extend(["-t", f"{seg['fr_end'] - seg['fr_start']:.3f}"])
        cmd.extend([
            "-i", str(source_path), "-vn",
            "-af", f"atempo={seg['atempo']:.6f}",
            "-c:a", "aac", "-b:a", "192k",
            str(seg_path),
        ])
        fr_end_str = f"{seg['fr_end']:.1f}s" if seg["fr_end"] is not None else "fin"
        _log(f"  ({lang.upper()}) Segment {i+1}/{len(segments)}: "
             f"src {seg['fr_start']:.1f}→{fr_end_str} atempo={seg['atempo']:.5f}x")
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode != 0:
                _log(f"  ❌ Segment {i+1} ffmpeg: {result.stderr[-300:]}")
                return False
            if not seg_path.exists() or seg_path.stat().st_size < 1000:
                _log(f"  ❌ Segment {i+1} fichier vide ou absent")
                return False
        except subprocess.TimeoutExpired:
            _log(f"  ❌ Segment {i+1} timeout")
            return False
        seg_files.append(seg_path)

    if len(seg_files) == 1:
        shutil.copy2(str(seg_files[0]), str(output_path))
        return True

    # Concat via the concat demuxer + stream copy — avoids re-encoding the already-AAC
    # segments, which prevents the EINVAL / channel-layout failures with EAC3 5.1 sources.
    n = len(seg_files)
    list_path = tmp_dir / f"concat_{lang}.txt"
    list_path.write_text("".join(f"file '{sf.absolute()}'\n" for sf in seg_files))
    concat_cmd = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", str(list_path),
        "-c:a", "copy",
        str(output_path),
    ]
    _log(f"  ({lang.upper()}) Concat {n} segments...")
    try:
        result = subprocess.run(concat_cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            _log(f"  ❌ Concat ffmpeg: {result.stderr[-300:]}")
            return False
    except subprocess.TimeoutExpired:
        _log(f"  ❌ Concat timeout")
        return False
    return True


def _correct_audio_speed(sec_path: Path, output_path: Path,
                          speed_factor: float, trim_start_sec: float = 0.0) -> bool:
    """Corrige la vitesse de la piste audio secondaire via ffmpeg atempo.

    Produit un fichier .mka en AAC pour compatibilité maximale avec Plex.
    atempo accepte [0.5, 2.0] — couverture largement suffisante pour les cas réels.
    """
    cmd = ["ffmpeg", "-y"]
    if trim_start_sec > 0.05:
        cmd.extend(["-ss", f"{trim_start_sec:.3f}"])
    cmd.extend(["-i", str(sec_path), "-vn"])
    cmd.extend(["-af", f"atempo={speed_factor:.6f}"])
    cmd.extend(["-c:a", "aac", "-b:a", "192k"])
    cmd.append(str(output_path))
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            logger.warning("atempo failed: %s", result.stderr[-300:])
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        return False


def _get_audio_track_id(path: Path) -> int:
    """Retourne le track ID mkvmerge de la première piste audio (défaut : 1)."""
    try:
        result = subprocess.run(
            ["mkvmerge", "-i", str(path)],
            capture_output=True, text=True, timeout=30,
        )
        for line in result.stdout.splitlines():
            m = re.match(r"Track ID (\d+):.*audio", line, re.IGNORECASE)
            if m:
                return int(m.group(1))
    except Exception:
        pass
    return 1


def _mux_multi_audio(
    ref_video: VideoInfo,
    ref_lang: str,
    secondary_audios: list,  # [(video_path, lang, source_VideoInfo, offset_sec), ...]
    output_path: Path,
) -> bool:
    """Muxe via mkvmerge — le délai est stocké dans le header MKV (pas en timestamp).
      < 0 → -ss (avance, coupe le début)
    Les fichiers secondaires sont les vidéos sources originales ; seul l'audio est mappé.
    """
    ref_lang_code = LANG_CODES.get(ref_lang, "und")
    ref_audio_tid = _get_audio_track_id(ref_video.path)

    # mkvmerge stocke le délai dans le header MKV (CodecDelay/TrackDelay),
    # contrairement à ffmpeg -itsoffset qui manipule les timestamps — Plex
    # gère nativement le format mkvmerge.
    cmd = [
        "mkvmerge", "-o", str(output_path),
        "--language", f"{ref_audio_tid}:{ref_lang_code}",
        str(ref_video.path),
    ]

    VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts"}
    for sec_path, sec_lang, _, offset_sec in secondary_audios:
        sec_lang_code = LANG_CODES.get(sec_lang, "und")
        sec_name = LANG_NAMES.get(sec_lang, sec_lang)
        sec_audio_tid = _get_audio_track_id(sec_path)

        if sec_path.suffix.lower() in VIDEO_EXTS:
            cmd.append("--no-video")
        cmd.extend(["--language", f"{sec_audio_tid}:{sec_lang_code}"])
        cmd.extend(["--track-name", f"{sec_audio_tid}:{sec_name}"])
        if abs(offset_sec) > 0.05:
            # offset > 0 = secondaire en avance (content plus tôt dans le fichier)
            #             → retarder de offset secondes dans le mux (+delay_ms)
            # offset < 0 = secondaire en retard → avancer (–delay_ms = négatif = trim)
            delay_ms = int(offset_sec * 1000)
            cmd.extend(["--sync", f"{sec_audio_tid}:{delay_ms}"])
        cmd.append(str(sec_path))

    _log(f"  mkvmerge: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        # mkvmerge: 0=succès, 1=avertissements, 2=erreur
        if result.returncode == 2:
            _log(f"  ❌ mkvmerge: {result.stdout[-500:]}")
            return False
        for line in result.stdout.splitlines():
            if any(k in line.lower() for k in ("warning", "error")):
                _log(f"  ⚠️ mkvmerge: {line}")
        return True
    except subprocess.TimeoutExpired:
        _log("  ❌ mkvmerge timeout")
        return False


# ---------------------------------------------------------------------------
# Permissions helpers
# ---------------------------------------------------------------------------

def _apply_media_permissions(path: Path, is_dir: bool = False) -> None:
    """Applique owner + permissions du répertoire parent au fichier/dossier créé.

    Dossiers : 775 (owner+group peuvent créer/supprimer des fichiers)
    Fichiers  : 664 (owner+group peuvent lire/écrire)
    """
    try:
        st = path.parent.stat()
        os.chown(path, st.st_uid, st.st_gid)
        os.chmod(path, 0o775 if is_dir else 0o664)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Merge workflow
# ---------------------------------------------------------------------------

def _merge_pair(pair: MediaPair, output_dir: Path, originals_dir: Path,
                dry_run: bool, preferred_ref_lang: str = "",
                sync_points: Optional[list] = None,
                prev_offset_ms: float = 0.0,
                prev_speed: float = 1.0,
                force_ref: bool = False,
                hint_offset_ms: float = 0.0,
                hint_atempo: float = 0.0) -> bool:
    # Quality-first: preferred_ref_lang only wins if it has the best quality score,
    # or if force_ref=True (manual mode — user explicitly chose the reference file).
    best = pair.reference
    best_lang = pair.ref_lang
    if preferred_ref_lang and preferred_ref_lang in pair.files:
        candidate = pair.files[preferred_ref_lang]
        if force_ref or candidate.quality_score >= best.quality_score:
            ref_lang = preferred_ref_lang
            ref = candidate
            forced = True
        else:
            ref = best
            ref_lang = best_lang
            forced = False
            _log(f"  ℹ️ Préférence '{preferred_ref_lang.upper()}' ignorée — "
                 f"'{ref_lang.upper()}' a une meilleure qualité "
                 f"({best.quality_score:,} vs {candidate.quality_score:,})")
    else:
        ref = best
        ref_lang = best_lang
        forced = False
    sec_items = {lang: v for lang, v in pair.files.items() if lang != ref_lang}

    _log(f"━━━ {pair.match_key} ({pair.match_type})")
    suffix = " [forcé]" if forced else " [auto]"
    _log(f"  Référence ({ref_lang.upper()}){suffix}: {ref.path.name}")
    _log(f"  {ref.width}×{ref.height}, "
         f"{ref.video_bitrate // 1000 if ref.video_bitrate else '?'}kbps, "
         f"score {ref.quality_score:,}")
    for lang, vinfo in sec_items.items():
        _log(f"  Secondaire ({lang.upper()}): {vinfo.path.name}")
        _log(f"    Δ durée: {abs(ref.duration - vinfo.duration):.1f}s")

    if dry_run:
        _log("  [DRY RUN] aucune action")
        return True

    output_name = ref.path.name
    for patterns in EDITION_PATTERNS.values():
        for pattern in patterns:
            output_name = re.sub(pattern, "", output_name, flags=re.IGNORECASE)
    output_name = re.sub(r"\s*-\s*-\s*", " - ", output_name)
    output_name = re.sub(r"\s{2,}", " ", output_name)
    # Strip trailing dots/spaces from stem (e.g. "{edition-English}." → residual ".")
    _p = Path(output_name)
    output_name = _p.stem.rstrip(". ").strip() + _p.suffix

    output_path = output_dir / output_name
    # En mode manuel le fichier ref et l'output auraient le même nom → suffixe
    input_paths = {v.path.resolve() for v in pair.files.values()}
    if output_path.resolve() in input_paths:
        output_name = output_path.stem + " [Multi]" + output_path.suffix
        output_path = output_dir / output_name
    if output_path.exists():
        _log(f"  ⚠️ Fichier de sortie existe déjà, ignoré: {output_name}")
        return False

    if sync_points:
        # ── Mode segmenté : mesures manuelles Audition ────────────────────────
        prev_offset_sec = prev_offset_ms / 1000.0
        _log(f"  Mode segmenté: {len(sync_points)} point(s) | "
             f"offset précédent {prev_offset_sec:.3f}s | vitesse précédente {prev_speed:.5f}x")
        segments = _compute_segments(sync_points, prev_offset_sec, prev_speed)
        for j, seg in enumerate(segments):
            fr_end_str = f"{seg['fr_end']:.1f}s" if seg["fr_end"] is not None else "fin"
            _log(f"  Segment {j+1}: src FR {seg['fr_start']:.1f}→{fr_end_str}"
                 f" | atempo={seg['atempo']:.5f}x")

        with tempfile.TemporaryDirectory(prefix="multi_audio_", dir=output_dir) as tmpdir:
            tmp = Path(tmpdir)
            secondary_audios_final = []
            for lang, vinfo in sec_items.items():
                corrected = tmp / f"segmented_{lang}.mka"
                if not _apply_segmented_correction(vinfo.path, segments, corrected, tmp, lang):
                    _log(f"  ❌ Échec correction segmentée ({lang.upper()})")
                    return False
                secondary_audios_final.append((corrected, lang, vinfo, prev_offset_sec))

            tmp_output = tmp / output_name
            _log("  → Fusion finale (segmentée)...")
            if not _mux_multi_audio(ref_video=ref, ref_lang=ref_lang,
                                     secondary_audios=secondary_audios_final,
                                     output_path=tmp_output):
                _log("  ❌ Échec du muxing")
                return False

            if tmp_output.stat().st_size < ref.file_size * 0.8:
                _log("  ⚠️ Fichier de sortie semble trop petit")

            shutil.move(str(tmp_output), str(output_path))
            _apply_media_permissions(output_path)

    else:
        # ── Analyse sync automatique multi-segments ───────────────────────────
        sync_results = []
        for lang, vinfo in sec_items.items():
            delta = abs(ref.duration - vinfo.duration)
            _log(f"  ({lang.upper()}) Δ durée: {delta:.1f}s — analyse multi-segments...")
            offset, segs = _analyze_multi_segment_sync(
                ref.path, vinfo.path, ref.duration, vinfo.duration,
                hint_offset_ms=hint_offset_ms,
                hint_atempo=hint_atempo,
            )
            if not segs:
                # Fallback: single constant-speed correction
                _log(f"  ({lang.upper()}) Fallback → analyse vitesse constante...")
                offset, speed = _analyze_audio_sync(
                    ref.path, vinfo.path, ref.duration, vinfo.duration
                )
                drift_pct = abs(speed - 1.0) * 100
                _log(f"  ({lang.upper()}) Offset: {offset:+.3f}s | Vitesse: {speed:.5f}x"
                     f"{'  ⚠️ ' + f'{drift_pct:.3f}%' if drift_pct > 0.01 else ''}")
                segs = [{"fr_start": 0.0, "fr_end": None, "atempo": speed}] if abs(speed - 1.0) > 0.0001 else []
            sync_results.append((lang, vinfo, offset, segs))

        with tempfile.TemporaryDirectory(prefix="multi_audio_", dir=output_dir) as tmpdir:
            tmp = Path(tmpdir)
            secondary_audios_final = []
            for lang, vinfo, offset, segs in sync_results:
                if segs:
                    corrected = tmp / f"auto_{lang}.mka"
                    if not _apply_segmented_correction(vinfo.path, segs, corrected, tmp, lang):
                        _log(f"  ❌ Échec correction ({lang.upper()})")
                        return False
                    secondary_audios_final.append((corrected, lang, vinfo, offset))
                else:
                    secondary_audios_final.append((vinfo.path, lang, vinfo, offset))

            tmp_output = tmp / output_name
            _log("  → Fusion finale...")
            if not _mux_multi_audio(ref_video=ref, ref_lang=ref_lang,
                                     secondary_audios=secondary_audios_final,
                                     output_path=tmp_output):
                _log("  ❌ Échec du muxing")
                return False

            if tmp_output.stat().st_size < ref.file_size * 0.8:
                _log("  ⚠️ Fichier de sortie semble trop petit")

            shutil.move(str(tmp_output), str(output_path))
            _apply_media_permissions(output_path)

    originals_dir.mkdir(parents=True, exist_ok=True)
    _apply_media_permissions(originals_dir, is_dir=True)
    for lang, vinfo in {ref_lang: ref, **sec_items}.items():
        dest = originals_dir / vinfo.path.name
        if dest.exists():
            dest = originals_dir / f"{vinfo.path.stem}_dup{vinfo.path.suffix}"
        shutil.move(str(vinfo.path), str(dest))
        _apply_media_permissions(dest)
        _log(f"  Original déplacé: {vinfo.path.name} → originals/")

    _log(f"  ✅ Fusion réussie: {output_name}")
    return True


# ---------------------------------------------------------------------------
# Mode manuel — paire explicite
# ---------------------------------------------------------------------------

def _manual_pair(file_ref: str, file_sec: str,
                 lang_ref: str = "", lang_sec: str = "") -> Optional[tuple]:
    ref_path = Path(file_ref)
    sec_path = Path(file_sec)
    if not ref_path.is_file():
        _log(f"❌ Fichier référence introuvable: {file_ref}")
        return None
    if not sec_path.is_file():
        _log(f"❌ Fichier secondaire introuvable: {file_sec}")
        return None
    if not lang_ref:
        lang_ref = _detect_language(ref_path.name) or "und"
    if not lang_sec:
        lang_sec = _detect_language(sec_path.name) or "und"
    if lang_ref == lang_sec:
        lang_sec = lang_sec + "_2"
    pair = MediaPair(
        files={
            lang_ref: _get_video_info(ref_path),
            lang_sec: _get_video_info(sec_path),
        },
        match_key=ref_path.stem,
        match_type="manuel",
    )
    return pair, lang_ref


# ---------------------------------------------------------------------------
# Job worker
# ---------------------------------------------------------------------------

def _run_job(directory: str, dry_run: bool, recursive: bool,
             preferred_ref_lang: str = "",
             mode: str = "auto",
             file_ref: str = "", file_sec: str = "",
             lang_ref: str = "", lang_sec: str = "",
             sync_points: Optional[list] = None,
             prev_offset_ms: float = 0.0,
             prev_speed: float = 1.0,
             tmdb_api_key: str = "",
             hint_offset_ms: float = 0.0,
             hint_atempo: float = 0.0) -> None:
    try:
        prod = "DRY RUN" if dry_run else "PRODUCTION"

        if mode == "manual":
            _log(f"Mode: MANUEL | {prod}")
            result = _manual_pair(file_ref, file_sec, lang_ref, lang_sec)
            if result is None:
                return
            pair, forced_ref_lang = result
            with _lock:
                _state["total"] = 1
            output_dir = Path(file_ref).parent
            try:
                ok = _merge_pair(pair, output_dir=output_dir,
                                 originals_dir=output_dir / "originals",
                                 dry_run=dry_run,
                                 preferred_ref_lang=forced_ref_lang,
                                 sync_points=sync_points,
                                 prev_offset_ms=prev_offset_ms,
                                 prev_speed=prev_speed,
                                 force_ref=True)
                with _lock:
                    _state["processed"] += 1
                    if ok:
                        _state["success"] += 1
                    else:
                        _state["failed"] += 1
            except Exception as e:
                logger.exception("Erreur merge manuel")
                _log(f"❌ Erreur: {e}")
                with _lock:
                    _state["processed"] += 1
                    _state["failed"] += 1

        else:
            dir_path = Path(directory)
            if preferred_ref_lang:
                ref_label = LANG_NAMES.get(preferred_ref_lang, preferred_ref_lang) + " [forcé]"
            elif tmdb_api_key:
                ref_label = "TMDB (langue d'origine)"
            else:
                ref_label = "auto"
            _log(f"Dossier: {directory}")
            _log(f"Mode: AUTO | {prod} | "
                 f"Récursif: {'oui' if recursive else 'non'} | "
                 f"Référence: {ref_label}")

            dirs_to_process = []
            if recursive:
                for root, dirs, files in os.walk(dir_path):
                    dirs[:] = [d for d in dirs if d != "originals"]
                    if any(f.lower().endswith(".mkv") for f in files):
                        dirs_to_process.append(Path(root))
            else:
                dirs_to_process = [dir_path]

            tmdb_cache: dict[str, str] = {}
            all_pairs: list[tuple[MediaPair, Path, str]] = []
            for d in dirs_to_process:
                _log(f"Scan: {d}")
                eff_ref = preferred_ref_lang
                if not eff_ref and tmdb_api_key:
                    title = _extract_search_title(d)
                    if title:
                        if title not in tmdb_cache:
                            found = _tmdb_original_language(title, tmdb_api_key)
                            tmdb_cache[title] = found or ""
                            if found:
                                _log(f"  TMDB '{title}': langue d'origine '{found}'")
                            else:
                                _log(f"  TMDB '{title}': introuvable, fallback 'en'")
                        eff_ref = tmdb_cache.get(title) or ""
                dir_fallback = eff_ref or "en"
                pairs = _find_pairs(d, default_lang=dir_fallback)
                all_pairs.extend((p, d, eff_ref) for p in pairs)

            with _lock:
                _state["total"] = len(all_pairs)

            if not all_pairs:
                _log("Aucune paire multi-langue trouvée.")
            else:
                _log(f"{len(all_pairs)} paire(s) trouvée(s)")

            for pair, d, eff_ref in all_pairs:
                try:
                    ok = _merge_pair(pair, output_dir=d,
                                     originals_dir=d / "originals",
                                     dry_run=dry_run,
                                     preferred_ref_lang=preferred_ref_lang,
                                     hint_offset_ms=hint_offset_ms,
                                     hint_atempo=hint_atempo)
                    with _lock:
                        _state["processed"] += 1
                        if ok:
                            _state["success"] += 1
                        else:
                            _state["failed"] += 1
                except Exception as e:
                    logger.exception("Erreur inattendue pour '%s'", pair.match_key)
                    _log(f"  ❌ Erreur inattendue: {e}")
                    with _lock:
                        _state["processed"] += 1
                        _state["failed"] += 1

            with _lock:
                s, f = _state["success"], _state["failed"]
            _log(f"━━━ RÉSUMÉ: {s} réussies, {f} échouées sur {len(all_pairs)} paires")

    except Exception as e:
        logger.exception("Erreur fatale multi-audio job")
        _log(f"❌ Erreur fatale: {e}")
    finally:
        with _lock:
            _state["running"] = False
            _state["done"] = True


def start_multi_audio_job(directory: str = "", dry_run: bool = True,
                          recursive: bool = False, preferred_ref_lang: str = "",
                          mode: str = "auto",
                          file_ref: str = "", file_sec: str = "",
                          lang_ref: str = "", lang_sec: str = "",
                          sync_points: Optional[list] = None,
                          prev_offset_ms: float = 0.0,
                          prev_speed: float = 1.0,
                          tmdb_api_key: str = "",
                          hint_offset_ms: float = 0.0,
                          hint_atempo: float = 0.0) -> bool:
    with _lock:
        if _state["running"]:
            return False
        _state.update({
            "running": True,
            "done": False,
            "log": deque(maxlen=500),
            "success": 0,
            "failed": 0,
            "total": 0,
            "processed": 0,
        })
    threading.Thread(
        target=_run_job,
        kwargs=dict(
            directory=directory, dry_run=dry_run, recursive=recursive,
            preferred_ref_lang=preferred_ref_lang,
            mode=mode, file_ref=file_ref, file_sec=file_sec,
            lang_ref=lang_ref, lang_sec=lang_sec,
            sync_points=sync_points, prev_offset_ms=prev_offset_ms,
            prev_speed=prev_speed, tmdb_api_key=tmdb_api_key,
            hint_offset_ms=hint_offset_ms,
            hint_atempo=hint_atempo,
        ),
        daemon=True,
    ).start()
    return True


# ---------------------------------------------------------------------------
# Dépendances
# ---------------------------------------------------------------------------

def check_multi_audio_deps() -> list[str]:
    return [tool for tool in ["ffmpeg", "ffprobe", "alass"]
            if shutil.which(tool) is None]
