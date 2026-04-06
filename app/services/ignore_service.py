"""
Service de gestion de la liste d'exclusion (ignore list).

Fichier : cache/ignore_list.json
Structure :
[
  {"title": "Beverly Hills, 90210", "type": "show", "reason": "double épisodes"},
  {"title": "The Blind", "type": "movie", "reason": "faux positif"}
]
"""

import json
import os
import threading

from app.config_paths import CACHE_DIR

IGNORE_FILE = os.path.join(CACHE_DIR, "ignore_list.json")
_lock = threading.Lock()

_cache: list | None = None
_mtime: float | None = None


def _load() -> list:
    global _cache, _mtime
    try:
        mtime = os.path.getmtime(IGNORE_FILE) if os.path.exists(IGNORE_FILE) else None
    except OSError:
        mtime = None

    with _lock:
        if _cache is not None and _mtime == mtime:
            return list(_cache)

    try:
        if os.path.exists(IGNORE_FILE):
            with open(IGNORE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = []
    except Exception:
        data = []

    with _lock:
        _cache = data
        _mtime = mtime

    return list(data)


def _save(entries: list) -> None:
    global _cache, _mtime
    os.makedirs(os.path.dirname(IGNORE_FILE), exist_ok=True)
    with _lock:
        with open(IGNORE_FILE, "w", encoding="utf-8") as f:
            json.dump(entries, f, indent=2, ensure_ascii=False)
        _cache = list(entries)
        try:
            _mtime = os.path.getmtime(IGNORE_FILE)
        except OSError:
            _mtime = None


def load_ignore_list() -> list:
    return _load()


def is_ignored(title: str, item_type: str = "") -> bool:
    entries = _load()
    title_lower = (title or "").strip().lower()
    for e in entries:
        if e.get("title", "").strip().lower() == title_lower:
            if not item_type or not e.get("type") or e.get("type") == item_type:
                return True
    return False


def add_ignore(title: str, item_type: str = "", reason: str = "") -> bool:
    entries = _load()
    title_clean = (title or "").strip()
    if not title_clean:
        return False
    # Ne pas dupliquer
    for e in entries:
        if e.get("title", "").strip().lower() == title_clean.lower():
            return False
    entries.append({
        "title": title_clean,
        "type": item_type or "",
        "reason": reason or "",
    })
    _save(entries)
    return True


def remove_ignore(title: str) -> bool:
    entries = _load()
    title_lower = (title or "").strip().lower()
    new_entries = [e for e in entries if e.get("title", "").strip().lower() != title_lower]
    if len(new_entries) == len(entries):
        return False
    _save(new_entries)
    return True
