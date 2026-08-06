#!/usr/bin/env python3
"""
restore_titles.py — Restaure les apostrophes et accents dans les noms de fichiers vidéo
en interrogeant TMDb pour obtenir le titre officiel.

Usage:
  python restore_titles.py /chemin/films [options]

Options:
  --api-key KEY    Clé API TMDb (ou variable d'env TMDB_API_KEY)
  --execute        Effectue réellement les renommages (dry-run par défaut)
  --recursive      Traite les sous-dossiers
  --lang LANG      Langue TMDb pour le titre (défaut: fr-CA, fallback: en-US)
  --filter TEXT    Traite seulement les fichiers dont le nom contient ce texte
  --tv             Cherche uniquement dans les séries TV (par défaut: auto)
  --movie          Cherche uniquement dans les films (par défaut: auto)
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

# ─── Constants ────────────────────────────────────────────────────────────────

VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts", ".webm", ".wmv", ".flv"}

ACCENT_MAP = str.maketrans(
    "àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ"
    "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ",
    "aaaaaaeceeeeiiiidnoooooouuuuytÿ"
    "AAAAAAECEEEEIIIIÐNOOOOOOUUUUYÞ"
)

# Characters invalid in Windows filenames
_INVALID_WIN_CHARS = re.compile(r'[<>:"/\\|?*]')

# ─── Helpers ──────────────────────────────────────────────────────────────────

def normalize(text):
    """Reduce to pure alphanumeric lowercase for comparison."""
    return re.sub(r'[^a-z0-9]', '', text.lower())


def to_ascii_search(text):
    """Convert accented chars to ASCII equivalents, strip apostrophes — for TMDb search."""
    result = text.replace('’', "'").replace('‘', "'")
    result = result.translate(ACCENT_MAP)
    result = result.encode("ascii", "ignore").decode("ascii")
    result = result.replace("'", " ").replace('"', " ").replace("`", " ")
    return re.sub(r'\s+', ' ', result).strip()


def safe_filename(title):
    """Replace Windows-invalid characters while keeping apostrophes and accents."""
    result = _INVALID_WIN_CHARS.sub(' ', title)
    result = re.sub(r'\s+', ' ', result).strip()
    return result


def tmdb_request(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return None


def tmdb_search(title, year, is_series, api_key, lang="fr-CA"):
    """
    Search TMDb for the best match.
    Returns (tmdb_id, media_type) or (None, None).
    """
    media_type = "tv" if is_series else "movie"
    title_norm = normalize(title)

    for search_lang in [lang, "en-US"]:
        params = {
            "api_key": api_key,
            "query": title,
            "language": search_lang,
            "include_adult": "false",
        }
        if year:
            params["year" if not is_series else "first_air_date_year"] = year
        url = f"https://api.themoviedb.org/3/search/{media_type}?" + urllib.parse.urlencode(params)
        data = tmdb_request(url)
        if not data:
            continue
        results = data.get("results") or []

        # Pass 1: exact match
        for r in results[:10]:
            r_title = r.get("title") or r.get("name") or ""
            if normalize(r_title) == title_norm:
                return r["id"], media_type

        # Pass 2: substring, prefer shortest (closest length)
        candidates = []
        for r in results[:10]:
            r_title = r.get("title") or r.get("name") or ""
            r_norm = normalize(r_title)
            if title_norm in r_norm or r_norm in title_norm:
                candidates.append((abs(len(r_norm) - len(title_norm)), -r.get("popularity", 0), r["id"]))
        if candidates:
            candidates.sort()
            return candidates[0][2], media_type

    return None, None


def tmdb_get_title(tmdb_id, media_type, api_key, lang="fr-CA"):
    """
    Fetch the official title from TMDb.
    Returns the title string or None.
    Tries lang first, falls back to en-US.
    """
    for fetch_lang in [lang, "en-US"]:
        url = (f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}"
               f"?api_key={api_key}&language={fetch_lang}")
        data = tmdb_request(url)
        if not data:
            continue
        title = data.get("title") or data.get("name") or ""
        if title:
            return title.strip()
    return None


# ─── Filename parsing ─────────────────────────────────────────────────────────

_TECH_BOUNDARY = re.compile(
    r'(?:^|\s)(?:2160|1080|720|576|480|432|360)p(?=[\s.\-]|$)'
    r'|(?:^|\s)(?:WEB-DL|WEBRip|BluRay|DVDRip|BDRip|HDTV|MULTi|REMUX)(?=[\s.\-]|$)',
    re.IGNORECASE,
)
_EP_PATTERN = re.compile(r'\s+[Ss]\d{1,2}[Ee]\d{1,3}')


def extract_title_year(stem):
    """
    Extract (title_in_stem, year_or_None, is_series) from a scene-named filename stem.
    title_in_stem is the exact substring at the start of stem representing the title.
    """
    s = stem

    # Series episode: title is everything before SxxExx
    ep_m = _EP_PATTERN.search(s)
    if ep_m:
        title = s[:ep_m.start()].strip()
        return title, None, True

    # Find tech tag boundary
    tech_m = _TECH_BOUNDARY.search(s)
    before_tech = s[:tech_m.start()].strip() if (tech_m and tech_m.start() > 0) else s

    # Remove subtitle (everything after " - ")
    dash_m = re.search(r' - .+$', before_tech)
    if dash_m:
        before_tech = before_tech[:dash_m.start()].strip()

    # Extract year from before_tech
    yr_m = re.search(r'\b((?:19|20)\d{2})\b', before_tech)
    if yr_m and yr_m.start() > 0:
        title = before_tech[:yr_m.start()].strip().rstrip('([').strip()
        year = yr_m.group(1)
    else:
        title = before_tech.strip()
        year = None

    # Remove parenthesized year if it leaked into title
    title = re.sub(r'\s*[\(\[]\d{4}[\)\]]\s*$', '', title).strip()

    return title, year, False


# ─── Core processing ──────────────────────────────────────────────────────────

def process_file(filepath, api_key, dry_run, lang, force_tv, force_movie, stats):
    filepath = Path(filepath)
    stem = filepath.stem

    old_title, year, is_series = extract_title_year(stem)
    if not old_title:
        stats["skipped"] += 1
        return

    if force_tv:
        is_series = True
    elif force_movie:
        is_series = False

    # Use ASCII-normalized title for TMDb search
    search_title = to_ascii_search(old_title)

    tmdb_id, media_type = tmdb_search(search_title, year, is_series, api_key, lang)
    time.sleep(0.25)

    if not tmdb_id:
        print(f"  ⚠  Introuvable : {old_title!r} ({year or '?'})")
        stats["not_found"] += 1
        return

    new_title_raw = tmdb_get_title(tmdb_id, media_type, api_key, lang)
    if not new_title_raw:
        stats["not_found"] += 1
        return

    new_title = safe_filename(new_title_raw)

    # Compare normalized versions — if they don't match the same content, skip
    if normalize(new_title) != normalize(old_title):
        # The TMDb result likely doesn't correspond to this file; skip to avoid bad renames
        print(f"  ⚠  Mismatch  : «{old_title}» ≠ «{new_title}» (normalisé) — ignoré")
        stats["mismatch"] += 1
        return

    # Check if there's actually something to fix
    if new_title == old_title:
        stats["already_ok"] += 1
        return

    # Replace the title portion at the start of the stem
    if not stem.lower().startswith(old_title.lower()):
        print(f"  ⚠  Position  : titre «{old_title}» introuvable au début de «{stem}»")
        stats["skipped"] += 1
        return

    new_stem = new_title + stem[len(old_title):]
    new_name = new_stem + filepath.suffix.lower()
    new_path = filepath.parent / new_name

    print(f"\n  📄 {filepath.name}")
    print(f"     {old_title!r:40s} → {new_title!r}")
    if not dry_run:
        print(f"     ✏  Nouveau nom : {new_name}")

    if dry_run:
        print(f"     🔍 [DRY RUN] → {new_name}")
        stats["would_rename"] += 1
        return

    if new_path.exists():
        print(f"     ⚠  Cible existe déjà, ignoré.")
        stats["skipped"] += 1
        return

    try:
        filepath.rename(new_path)
        stats["renamed"] += 1
        print(f"     ✅ Renommé !")
    except Exception as e:
        print(f"     ❌ Erreur : {e}")
        stats["errors"] += 1


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Restaure apostrophes et accents dans les noms de fichiers vidéo via TMDb."
    )
    parser.add_argument("path", help="Fichier ou dossier à traiter")
    parser.add_argument("--api-key", default=os.environ.get("TMDB_API_KEY", ""),
                        help="Clé API TMDb")
    parser.add_argument("--execute", action="store_true",
                        help="Effectue les renommages (dry-run par défaut)")
    parser.add_argument("--recursive", "-r", action="store_true",
                        help="Traite les sous-dossiers")
    parser.add_argument("--lang", default="fr-CA",
                        help="Langue TMDb (défaut: fr-CA)")
    parser.add_argument("--filter", dest="name_filter", default="",
                        help="Ne traite que les fichiers dont le nom contient ce texte")
    parser.add_argument("--tv", action="store_true", help="Cherche uniquement les séries TV")
    parser.add_argument("--movie", action="store_true", help="Cherche uniquement les films")
    args = parser.parse_args()

    if not args.api_key:
        sys.exit("❌ Clé API TMDb manquante. Utilisez --api-key ou définissez TMDB_API_KEY.")

    if args.tv and args.movie:
        sys.exit("❌ --tv et --movie sont mutuellement exclusifs.")

    dry_run = not args.execute
    target = Path(args.path)

    if not target.exists():
        sys.exit(f"❌ Chemin introuvable : {target}")

    if target.is_file():
        video_files = [target] if target.suffix.lower() in VIDEO_EXTENSIONS else []
    else:
        pattern = "**/*" if args.recursive else "*"
        video_files = sorted(
            (f for f in target.glob(pattern)
             if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS),
            key=lambda f: f.name,
        )

    if args.name_filter:
        nf = args.name_filter.lower()
        video_files = [f for f in video_files if nf in f.stem.lower()]

    if not video_files:
        print("Aucun fichier vidéo trouvé.")
        return

    mode = "DRY RUN" if dry_run else "EXÉCUTION"
    print(f"\n{'='*60}")
    print(f" restore_titles.py — {mode}")
    print(f" {len(video_files)} fichier(s) | lang={args.lang} | récursif={args.recursive}")
    print(f"{'='*60}\n")

    stats = {
        "renamed": 0, "would_rename": 0, "already_ok": 0,
        "not_found": 0, "mismatch": 0, "skipped": 0, "errors": 0,
    }

    for i, filepath in enumerate(video_files, 1):
        print(f"[{i}/{len(video_files)}] {filepath.name}", end="")
        process_file(filepath, args.api_key, dry_run, args.lang,
                     args.tv, args.movie, stats)

    print(f"\n{'='*60}")
    if dry_run:
        print(f" ✏  À renommer  : {stats['would_rename']}")
    else:
        print(f" ✅ Renommés    : {stats['renamed']}")
    print(f" ✔  Déjà OK     : {stats['already_ok']}")
    print(f" ⚠  Introuvable : {stats['not_found']}")
    print(f" ⚠  Mismatch    : {stats['mismatch']}")
    print(f" ⏭  Ignorés     : {stats['skipped']}")
    print(f" ❌ Erreurs      : {stats['errors']}")
    print(f"{'='*60}\n")

    if dry_run and stats['would_rename'] > 0:
        print(f"Relancez avec --execute pour appliquer les {stats['would_rename']} renommages.\n")


if __name__ == "__main__":
    main()
