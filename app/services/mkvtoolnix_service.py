import os
import re
import json
import shutil
import subprocess
import time
import threading as _threading
from typing import Optional, Tuple, List, Dict
from flask import current_app
from app.config_paths import MKV_LANGUAGES_FILE as _MKV_LANGUAGES_FILE

_lang_lock = _threading.Lock()
_BASE_LANGUAGES = ["fr", "fr-CA", "en", "en-US", "es", "pt", "pt-BR", "de", "it", "ja", "ko", "zh", "und"]


def load_mkv_languages() -> list:
    with _lang_lock:
        try:
            if os.path.exists(_MKV_LANGUAGES_FILE):
                with open(_MKV_LANGUAGES_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    seen = set(_BASE_LANGUAGES)
                    result = list(_BASE_LANGUAGES)
                    for lang in data:
                        if lang and lang not in seen:
                            result.append(lang)
                            seen.add(lang)
                    return sorted(result, key=str.lower)
        except Exception:
            pass
        return sorted(_BASE_LANGUAGES, key=str.lower)


def register_mkv_languages(new_codes: list) -> None:
    with _lang_lock:
        existing = []
        try:
            if os.path.exists(_MKV_LANGUAGES_FILE):
                with open(_MKV_LANGUAGES_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    existing = [c for c in data if c]
        except Exception:
            pass
        seen = set(_BASE_LANGUAGES) | set(existing)
        added = False
        for code in new_codes:
            if code and code not in seen:
                existing.append(code)
                seen.add(code)
                added = True
        if added:
            try:
                with open(_MKV_LANGUAGES_FILE, "w", encoding="utf-8") as f:
                    json.dump(existing, f, indent=2, ensure_ascii=False)
            except Exception:
                pass


VIDEO_EXTS = (".mkv", ".mp4", ".avi", ".mov", ".mpeg")

class MKVCancelledError(Exception):
    pass

def fix_permissions(path: str):
    try:
        subprocess.run(["chmod", "777", path], check=False)
        subprocess.run(["chown", "nobody:users", path], check=False)
    except Exception as e:
        print(f"[PERMS] erreur sur {path}: {e}")

def get_mkvtoolnix_binaries(config) -> Tuple[Optional[str], Optional[str]]:
    """
    Retourne les chemins vers mkvmerge et mkvpropedit.
    Compatible Windows et Docker/Linux.
    """
    custom_bin = str(config.get("MKVTOOLNIX_BIN") or "").strip()

    def _is_exec(path: str) -> bool:
        return bool(path) and os.path.isfile(path) and os.access(path, os.X_OK)

    candidates = []

    if os.name == "nt":
        if custom_bin:
            candidates.append((
                os.path.join(custom_bin, "mkvmerge.exe"),
                os.path.join(custom_bin, "mkvpropedit.exe"),
            ))

        candidates.extend([
            (
                r"C:\Program Files\MKVToolNix\mkvmerge.exe",
                r"C:\Program Files\MKVToolNix\mkvpropedit.exe",
            ),
            (
                r"C:\Program Files (x86)\MKVToolNix\mkvmerge.exe",
                r"C:\Program Files (x86)\MKVToolNix\mkvpropedit.exe",
            ),
        ])

    else:
        if custom_bin:
            candidates.append((
                os.path.join(custom_bin, "mkvmerge"),
                os.path.join(custom_bin, "mkvpropedit"),
            ))

        candidates.extend([
            ("/usr/bin/mkvmerge", "/usr/bin/mkvpropedit"),
            ("/usr/local/bin/mkvmerge", "/usr/local/bin/mkvpropedit"),
            ("/bin/mkvmerge", "/bin/mkvpropedit"),
        ])

    for mkvmerge, mkvpropedit in candidates:
        if _is_exec(mkvmerge) and _is_exec(mkvpropedit):
            return mkvmerge, mkvpropedit

    which_merge = shutil.which("mkvmerge")
    which_propedit = shutil.which("mkvpropedit")

    if which_merge and which_propedit:
        return which_merge, which_propedit

    return None, None


def is_video_file(path: str) -> bool:
    return path.lower().endswith(VIDEO_EXTS)


def is_unc_path(path: str) -> bool:
    return path.startswith("\\\\") or path.startswith("//")


def to_safe_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*]+', "-", name).strip()


def to_win32_safe_path(path: str) -> str:
    if os.name == "nt":
        if path.startswith("\\\\"):
            return "\\\\?\\UNC\\" + path[2:]
        if not path.startswith("\\\\?\\"):
            return "\\\\?\\" + path
    return path


def extract_languages(file_path: str, mkvmerge_path: str) -> List[Dict]:
    """
    Retourne une liste de dicts {track_index, lang_code, label} pour les pistes audio.
    track_index est 1-based (pour mkvpropedit track:aN).
    """
    if not mkvmerge_path:
        return [{"track_index": 1, "lang_code": "erreur", "label": "Erreur: MKVToolNix introuvable"}]

    def _is_fr_ca_name(name_l: str) -> bool:
        return (
            "vfq" in name_l or "quebec" in name_l or "québec" in name_l or
            " canada" in name_l or "(qc" in name_l or "canadien" in name_l or
            "french (canada)" in name_l or "français (canada)" in name_l
        )

    iso639_2_to_1 = {
        "eng": "en",
        "fre": "fr", "fra": "fr",
        "ger": "de", "deu": "de",
        "spa": "es",
        "por": "pt",
        "ita": "it",
        "jpn": "ja",
        "chi": "zh", "zho": "zh",
    }

    try:
        safe_path = to_win32_safe_path(file_path)
        result = subprocess.run(
            [mkvmerge_path, "-J", safe_path],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=60,
        )

        if result.returncode != 0:
            return [{"track_index": 1, "lang_code": "erreur", "label": "Erreur MKVToolNix"}]

        metadata = json.loads(result.stdout)
        audio_tracks = [t for t in metadata.get("tracks", []) if t.get("type") == "audio"]
        langs = []
        audio_idx = 1  # 1-based for mkvpropedit track:aN

        for track in audio_tracks:
            props = track.get("properties", {}) or {}
            ietf = (props.get("language_ietf") or "").strip()
            raw = (props.get("language") or "").strip()
            name = (props.get("track_name") or "").strip()

            base = ietf or raw or "und"
            base_l = base.lower()
            name_l = name.lower()

            if len(base_l) == 3 and base_l in iso639_2_to_1:
                base_l = iso639_2_to_1[base_l]

            if _is_fr_ca_name(name_l) or base_l in ("fr-ca", "fr_ca"):
                base_l = "fr-ca"

            if base_l.startswith("en"):
                base_l = "en"
            if base_l.startswith("fr") and base_l not in ("fr", "fr-ca"):
                base_l = "fr"

            display = f"{base_l} ({name})" if name else base_l
            langs.append({"track_index": audio_idx, "lang_code": base_l, "label": display})
            audio_idx += 1

        register_mkv_languages([e["lang_code"] for e in langs])
        return langs

    except Exception:
        return []


def get_mkv_status(languages: List[Dict]) -> str:
    if not languages or any("erreur" in e.get("lang_code", "").lower() for e in languages):
        return "Erreur"
    codes = [e.get("lang_code", "").lower() for e in languages]
    if any(c == "fr-ca" for c in codes):
        return "OK"
    if any(c.startswith("fr") and c != "fr-ca" for c in codes):
        return "À traiter"
    return "Pas de FR"


def find_source_path(relative_path: str, source_dirs: list[str]):
    for base in source_dirs:
        attempt = os.path.join(base, relative_path)
        if os.path.exists(attempt):
            return attempt, base
    return None, None


def collect_video_files(src_path: str) -> List[str]:
    files_to_process = []

    if os.path.isfile(src_path):
        if is_video_file(src_path):
            files_to_process = [src_path]

    elif os.path.isdir(src_path):
        for root, dirs, files in os.walk(src_path):
            dirs[:] = [d for d in dirs if d != ".grab"]
            for f in files:
                if is_video_file(f):
                    files_to_process.append(os.path.join(root, f))

    return files_to_process


SEASON_DIR_RE = re.compile(r'^(?:s\d{1,3}|season\s*\d{1,3}|saison\s*\d{1,3})$', re.IGNORECASE)
EP_PATTERNS = [
    re.compile(r'[Ss](\d{1,3})[ ._-]?[Ee](\d{1,3})'),
    re.compile(r'(\d{1,3})x(\d{1,3})'),
    re.compile(r'(?:episode|épisode|ep)[ ._-]?(\d{1,3})', re.IGNORECASE),
]
PLATFORM_HINTS = {
    'netflix', 'disney+', 'disneyplus', 'prime video', 'amazon', 'primevideo', 'appletv', 'apple tv',
    'hbo', 'paramount+', 'paramountplus', 'tubi', 'clubillico', 'stacktv', 'teletoon+', 'plex', 'sync',
    'drm mdp', 'drmmdp'
}


def _is_season_dir(name: str) -> bool:
    n = (name or '').strip().lower().replace('_', ' ')
    return bool(SEASON_DIR_RE.match(n))


def _clean_series_name(name: str) -> str:
    if not name:
        return ''
    name = re.sub(r'\s*\(\s*\d{4}\s*\)\s*$', '', name).strip()
    name = name.replace('.', ' ').strip(' -_')
    return name


def infer_series_name_from_rel(rel_path: str) -> Optional[str]:
    parts = [p for p in rel_path.split(os.sep) if p]
    if not parts:
        return None

    dirs = parts[:-1]

    for i in range(len(dirs) - 1, -1, -1):
        if _is_season_dir(dirs[i]) and i > 0:
            candidate = _clean_series_name(dirs[i - 1])
            if candidate:
                return candidate

    for i in range(len(dirs) - 1, -1, -1):
        d = (dirs[i] or '').strip()
        dlow = d.lower()
        if d and (dlow not in PLATFORM_HINTS) and not _is_season_dir(d):
            candidate = _clean_series_name(d)
            if candidate:
                return candidate

    fname = parts[-1]
    for p in EP_PATTERNS:
        m = p.search(fname)
        if m:
            head = fname[:m.start()]
            candidate = _clean_series_name(head)
            if candidate:
                return candidate

    return None


def parse_episode_info(filename: str) -> Tuple[Optional[int], Optional[int], str]:
    base = os.path.splitext(os.path.basename(filename))[0]

    for p in EP_PATTERNS:
        m = p.search(base)
        if m:
            if len(m.groups()) >= 2:
                season = int(m.group(1))
                episode = int(m.group(2))
                title = base[m.end():].strip(" -._")
                return season, episode, title
            else:
                episode = int(m.group(1))
                title = base[m.end():].strip(" -._")
                return None, episode, title

    return None, None, base

def build_destination_path(
    src_file: str,
    src_root: str,
    dst_dir: str,
    is_series_category: bool,
    forced_series_name: str = "",
) -> str:
    filename = os.path.basename(src_file)

    if not is_series_category:
        return os.path.join(dst_dir, to_safe_filename(filename))

    # Priorité absolue : nom propre venant de Plex
    series_name = (forced_series_name or "").strip()

    # Fallback seulement si Plex n'a rien fourni
    if not series_name:
        rel = os.path.relpath(src_file, src_root)
        rel_parts = [p for p in os.path.normpath(rel).split(os.sep) if p and p != "."]
        src_root_name = os.path.basename(os.path.normpath(src_root)).strip()

        if len(rel_parts) >= 3 and _is_season_dir(rel_parts[-2]):
            series_name = rel_parts[-3]
        elif len(rel_parts) >= 2 and not _is_season_dir(rel_parts[-2]):
            series_name = rel_parts[-2]
        else:
            series_name = (
                infer_series_name_from_rel(rel)
                or infer_series_name_from_rel(src_root_name)
                or "Série inconnue"
            )

    season, episode, ep_title = parse_episode_info(filename)

    if season is not None:
        return os.path.join(
            dst_dir,
            to_safe_filename(series_name),
            f"Season {season:02d}",
            to_safe_filename(filename),
        )

    return os.path.join(
        dst_dir,
        to_safe_filename(series_name),
        to_safe_filename(filename),
    )

def probe_tracks(src_file: str, mkvmerge_path: str) -> Dict:
    """
    Retourne le JSON complet de mkvmerge -J pour un fichier vidéo.
    """
    cmd = [mkvmerge_path, "-J", to_win32_safe_path(src_file)]
    result = subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
    )
    return json.loads(result.stdout)

def choose_audio_track(meta: Dict) -> Optional[Dict]:
    def _name_indicates_fr_ca(name_l: str) -> bool:
        return (
            'vfq' in name_l or 'quebec' in name_l or 'québec' in name_l or
            ' canada' in name_l or '(qc' in name_l or 'canadien' in name_l or
            'french (canada)' in name_l or 'français (canada)' in name_l
        )

    audio_tracks = [t for t in meta.get("tracks", []) if t.get("type") == "audio"]

    for t in audio_tracks:
        props = t.get("properties", {}) or {}
        lang_ietf = (props.get("language_ietf") or "").lower()
        tname_l = (props.get("track_name") or "").lower()
        if "fr-ca" in lang_ietf or "fr_ca" in lang_ietf or _name_indicates_fr_ca(tname_l):
            return t

    for t in audio_tracks:
        props = t.get("properties", {}) or {}
        lang_ietf = (props.get("language_ietf") or "").lower()
        lang_raw = (props.get("language") or "").lower()
        if lang_ietf.startswith("fr") or lang_raw.startswith("fr"):
            return t

    return None

def run_command_cancelable(cmd, job_id, active_processes, cancel_flags, progress_lock, progress_callback=None):
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8"
    )

    with progress_lock:
        active_processes[job_id] = proc

    output_lines = []

    try:
        while True:
            line = proc.stdout.readline() if proc.stdout else ""
            if line:
                output_lines.append(line)
                if progress_callback:
                    m = re.match(r"Progress:\s*(\d+)%", line.strip())
                    if m:
                        try:
                            progress_callback(int(m.group(1)))
                        except Exception:
                            pass

            with progress_lock:
                cancelled = cancel_flags.get(job_id, False)

            if cancelled:
                try:
                    proc.terminate()
                except Exception:
                    pass
                raise MKVCancelledError("Traitement annulé")

            if proc.poll() is not None:
                break

            time.sleep(0.1)

        if proc.stdout:
            rest = proc.stdout.read()
            if rest:
                output_lines.append(rest)

        if proc.returncode != 0:
            raise subprocess.CalledProcessError(
                proc.returncode,
                cmd,
                output="".join(output_lines)
            )

        return "".join(output_lines)

    finally:
        with progress_lock:
            if active_processes.get(job_id) is proc:
                active_processes.pop(job_id, None)

def remux_file(
    src_file: str,
    dst_file: str,
    mkvmerge_path: str,
    mkvpropedit_path: str,
    job_id=None,
    active_processes=None,
    cancel_flags=None,
    progress_lock=None,
    progress_callback=None,
    language_overrides: dict = None,
) -> None:
    parent_dir = os.path.dirname(dst_file)
    os.makedirs(parent_dir, exist_ok=True)
    try:
        os.chmod(parent_dir, 0o777)
    except Exception:
        pass

    safe_src = to_win32_safe_path(src_file)
    safe_dst = to_win32_safe_path(dst_file)

    def _run(cmd, cb=None):
        if job_id and active_processes is not None and cancel_flags is not None and progress_lock is not None:
            return run_command_cancelable(cmd, job_id, active_processes, cancel_flags, progress_lock, progress_callback=cb)
        return subprocess.run(cmd, check=True, text=True, encoding="utf-8")

    def _name_indicates_fr_ca(name_l: str) -> bool:
        return (
            'vfq' in name_l or 'quebec' in name_l or 'québec' in name_l or
            ' canada' in name_l or '(qc' in name_l or 'canadien' in name_l or
            'french (canada)' in name_l or 'français (canada)' in name_l or
            'fr-ca' in name_l or 'fr ca' in name_l
        )

    def _is_same_track(remuxed_track: Dict, original_track: Dict) -> bool:
        rp = remuxed_track.get("properties", {}) or {}
        op = original_track.get("properties", {}) or {}

        r_lang = (rp.get("language_ietf") or rp.get("language") or "").lower()
        o_lang = (op.get("language_ietf") or op.get("language") or "").lower()

        r_name = (rp.get("track_name") or "").strip().lower()
        o_name = (op.get("track_name") or "").strip().lower()

        return r_lang == o_lang and r_name == o_name

    try:
        meta = probe_tracks(src_file, mkvmerge_path)
        target_track = choose_audio_track(meta)

        # Remux complet: on garde toutes les pistes
        cmd = [mkvmerge_path, "-o", safe_dst, safe_src]
        _run(cmd, cb=progress_callback)

        remuxed_meta = probe_tracks(dst_file, mkvmerge_path)
        remuxed_audio_tracks = [t for t in remuxed_meta.get("tracks", []) if t.get("type") == "audio"]
        remuxed_subs = [t for t in remuxed_meta.get("tracks", []) if t.get("type") == "subtitles"]

        selected_remuxed_track = None

        if target_track:
            target_props = target_track.get("properties", {}) or {}
            target_name_l = (target_props.get("track_name") or "").lower()
            target_lang_ietf = (target_props.get("language_ietf") or "").lower()
            target_lang = (target_props.get("language") or "").lower()

            # Tente d'abord de retrouver exactement la même piste après remux
            for rt in remuxed_audio_tracks:
                if _is_same_track(rt, target_track):
                    selected_remuxed_track = rt
                    break

            # Fallback sur le type de piste recherché
            if not selected_remuxed_track:
                target_is_fr_ca = (
                    "fr-ca" in target_lang_ietf or
                    target_lang in {"fr-ca", "fra-ca", "fre-ca"} or
                    _name_indicates_fr_ca(target_name_l)
                )

                for rt in remuxed_audio_tracks:
                    rp = rt.get("properties", {}) or {}
                    r_name_l = (rp.get("track_name") or "").lower()
                    r_lang_ietf = (rp.get("language_ietf") or "").lower()
                    r_lang = (rp.get("language") or "").lower()

                    if target_is_fr_ca:
                        if (
                            "fr-ca" in r_lang_ietf or
                            r_lang in {"fr-ca", "fra-ca", "fre-ca"} or
                            _name_indicates_fr_ca(r_name_l)
                        ):
                            selected_remuxed_track = rt
                            break
                    else:
                        if r_lang_ietf.startswith("fr") or r_lang.startswith("fr"):
                            selected_remuxed_track = rt
                            break

        # Fallback intelligent si aucune piste FR / FR-CA trouvée
        if not selected_remuxed_track and remuxed_audio_tracks:
            selected_remuxed_track = remuxed_audio_tracks[0]
            try:
                current_app.logger.warning(
                    "[MKV] Aucune piste FR/FR-CA trouvée dans '%s'; utilisation de la première piste audio disponible.",
                    os.path.basename(src_file)
                )
            except Exception:
                pass

        # Toutes les pistes audio à default=0
        for idx, _at in enumerate(remuxed_audio_tracks, start=1):
            _run([
                mkvpropedit_path, safe_dst,
                "--edit", f"track:a{idx}",
                "--set", "flag-default=0"
            ])

        # La piste choisie devient default=1
        if selected_remuxed_track:
            selected_index = None
            for idx, at in enumerate(remuxed_audio_tracks, start=1):
                if at.get("id") == selected_remuxed_track.get("id"):
                    selected_index = idx
                    break

            if selected_index is not None:
                props = selected_remuxed_track.get("properties", {}) or {}
                tname = props.get("track_name") or ""
                tname_l = tname.lower()

                is_fr_ca = (
                    "fr-ca" in (props.get("language_ietf", "").lower())
                    or (props.get("language", "").lower() in {"fr-ca", "fra-ca", "fre-ca"})
                    or _name_indicates_fr_ca(tname_l)
                )

                current_lang_ietf = (props.get("language_ietf") or "").lower()
                current_lang = (props.get("language") or "").lower()

                should_normalize_to_french = (
                    is_fr_ca or
                    current_lang_ietf.startswith("fr") or
                    current_lang.startswith("fr")
                )

                final_name = tname.strip() if tname and tname.strip() else (
                    "Français (Canada)" if is_fr_ca else "Français"
                )

                edit_cmd = [
                    mkvpropedit_path, safe_dst,
                    "--edit", f"track:a{selected_index}",
                    "--set", "flag-default=1",
                ]

                if should_normalize_to_french:
                    ietf_target = "fr-CA" if is_fr_ca else "fr-FR"
                    iso_target = "fra"
                    edit_cmd.extend([
                        "--set", f"language-ietf={ietf_target}",
                        "--set", f"language={iso_target}",
                    ])

                _run(edit_cmd)

                if should_normalize_to_french:
                    _run([
                        mkvpropedit_path, safe_dst,
                        "--edit", f"track:a{selected_index}",
                        "--delete", "name"
                    ])
                    _run([
                        mkvpropedit_path, safe_dst,
                        "--edit", f"track:a{selected_index}",
                        "--set", f"name={final_name}"
                    ])

                try:
                    current_app.logger.info(
                        "[MKV] Default audio set to: %s (track a%s) for '%s'",
                        final_name,
                        selected_index,
                        os.path.basename(src_file)
                    )
                except Exception:
                    pass

        # Désactiver les sous-titres par défaut sans toucher au forced
        for st in remuxed_subs:
            tid = st.get("id")
            if tid is not None:
                _run([
                    mkvpropedit_path, safe_dst,
                    "--edit", f"track:{tid}",
                    "--set", "flag-default=0"
                ])

        # Appliquer les overrides de langue utilisateur
        if language_overrides:
            _ISO1_TO_ISO3 = {
                "fr": "fra", "en": "eng", "es": "spa", "pt": "por",
                "de": "deu", "it": "ita", "ja": "jpn", "ko": "kor",
                "zh": "zho", "ru": "rus", "ar": "ara", "und": "und",
            }
            _LANG_NAMES = {
                "fr-ca": "Français (Canada)",
                "fr":    "Français",
                "en-us": "English (US)",
                "en-gb": "English (UK)",
                "en":    "English",
                "es":    "Español",
                "pt-br": "Português (Brasil)",
                "pt":    "Português",
                "de":    "Deutsch",
                "it":    "Italiano",
                "ja":    "日本語",
                "ko":    "한국어",
                "zh":    "中文",
                "ru":    "Русский",
                "ar":    "العربية",
                "nl":    "Nederlands",
                "fil":   "Filipino",
            }
            for str_idx, new_lang in (language_overrides or {}).items():
                try:
                    idx = int(str_idx)
                    new_lang = (new_lang or "").strip()
                    if not new_lang:
                        continue
                    base = new_lang.split("-")[0].lower()
                    iso3 = _ISO1_TO_ISO3.get(base, base[:3] if len(base) >= 3 else base)
                    track_name = _LANG_NAMES.get(new_lang.lower(), new_lang)
                    _run([
                        mkvpropedit_path, safe_dst,
                        "--edit", f"track:a{idx}",
                        "--set", f"language-ietf={new_lang}",
                        "--set", f"language={iso3}",
                        "--set", f"name={track_name}",
                    ])
                except Exception:
                    pass

        # Permissions finales du fichier remuxé
        fix_permissions(dst_file)

        # Permissions du dossier parent
        parent_dir = os.path.dirname(dst_file)
        if parent_dir and os.path.isdir(parent_dir):
            fix_permissions(parent_dir)

    except MKVCancelledError:
        if os.path.exists(dst_file):
            try:
                os.remove(dst_file)
            except Exception:
                pass
        raise