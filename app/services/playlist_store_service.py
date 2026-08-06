"""
playlist_store_service.py — Persistence des définitions de playlists.

Fichier : /config/playlists_store.json
Structure :
  {
    "playlists": [
      {
        "id":         "a1b2c3d4",          # 8-char hex
        "name":       "Middle Earth",
        "trakt_url":  "https://...",        # "" si créée manuellement
        "created_at": "2026-04-12T10:00:00",
        "updated_at": "2026-04-12T10:00:00",
        "items": [
          {
            "title":       "The Fellowship of the Ring",
            "year":        2001,
            "type":        "movie",          # movie | show | episode
            "tmdb_id":     120,
            "imdb_id":     "tt0120737",
            "tvdb_id":     null,
            "plex_key":    "12345",          # null = placeholder (pas encore dans Plex)
            "plex_title":  "...",
            "available":   true,
            "season":      null,             # pour les épisodes détachés
            "episode_num": null,
            "show_title":  null,
          }
        ]
      }
    ]
  }
"""
import json
import logging
import os
import uuid
from datetime import datetime

from app.config_paths import PLAYLISTS_STORE_FILE

logger = logging.getLogger(__name__)


def _load_store() -> dict:
    if not os.path.exists(PLAYLISTS_STORE_FILE):
        return {"playlists": []}
    try:
        with open(PLAYLISTS_STORE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.warning("[STORE] Impossible de lire playlists_store.json — réinitialisation")
        return {"playlists": []}


def _save_store(data: dict) -> None:
    os.makedirs(os.path.dirname(PLAYLISTS_STORE_FILE), exist_ok=True)
    with open(PLAYLISTS_STORE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def list_playlists() -> list:
    """Retourne les résumés (sans les items) de toutes les playlists sauvegardées."""
    store = _load_store()
    result = []
    for p in store["playlists"]:
        items = p.get("items", [])
        result.append({
            "id":        p["id"],
            "name":      p["name"],
            "trakt_url": p.get("trakt_url", ""),
            "updated_at": p.get("updated_at", ""),
            "total":     len(items),
            "available": sum(1 for it in items if it.get("available")),
            "pending":   sum(1 for it in items if not it.get("available")),
        })
    return result


def upsert_playlist(name: str, trakt_url: str, items: list) -> str:
    """
    Crée ou met à jour une playlist identifiée par son nom.
    Retourne l'ID de la playlist.
    """
    store = _load_store()
    now = datetime.now().isoformat()
    existing = next((p for p in store["playlists"] if p["name"] == name), None)
    if existing:
        existing["items"]      = items
        existing["trakt_url"]  = trakt_url or ""
        existing["updated_at"] = now
        pid = existing["id"]
        logger.info("[STORE] Playlist '%s' mise à jour (%d items)", name, len(items))
    else:
        pid = uuid.uuid4().hex[:8]
        store["playlists"].append({
            "id":         pid,
            "name":       name,
            "trakt_url":  trakt_url or "",
            "created_at": now,
            "updated_at": now,
            "items":      items,
        })
        logger.info("[STORE] Playlist '%s' créée (%d items)", name, len(items))
    _save_store(store)
    return pid


def get_playlist(pid: str) -> dict | None:
    store = _load_store()
    return next((p for p in store["playlists"] if p["id"] == pid), None)


def delete_playlist(pid: str) -> None:
    store = _load_store()
    before = len(store["playlists"])
    store["playlists"] = [p for p in store["playlists"] if p["id"] != pid]
    if len(store["playlists"]) < before:
        _save_store(store)
        logger.info("[STORE] Playlist '%s' supprimée", pid)
