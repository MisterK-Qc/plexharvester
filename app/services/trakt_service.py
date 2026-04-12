"""
trakt_service.py — Trakt API client (lecture publique, Client ID uniquement).

Supporte :
  • listes utilisateur  : trakt.tv/users/USERNAME/lists/SLUG
  • watchlists          : trakt.tv/users/USERNAME/watchlist
  • listes spéciales    : trakt.tv/movies/trending|popular|watched|anticipated
                          trakt.tv/shows/trending|popular|watched|anticipated
"""
import re
import logging

import requests

logger = logging.getLogger(__name__)

TRAKT_BASE = "https://api.trakt.tv"

_URL_RE = re.compile(
    r"trakt\.tv/users/(?P<user>[^/]+?)"
    r"(?:/lists/(?P<slug>[^/?#\s]+)|/(?P<wl>watchlist))"
    r"|trakt\.tv/(?P<media>movies|shows)/(?P<kind>trending|popular|watched|collected|anticipated)",
    re.IGNORECASE,
)


def _headers(client_id: str) -> dict:
    return {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
    }


# ── URL parsing ───────────────────────────────────────────────────────────────

def parse_trakt_url(url: str) -> dict:
    """
    Parse a Trakt URL.
    Returns a dict with key 'type' in ('list', 'watchlist', 'special').
    Raises ValueError for unrecognized URLs.
    """
    m = _URL_RE.search(url)
    if not m:
        raise ValueError(
            "URL Trakt non reconnue. "
            "Formats supportés : trakt.tv/users/X/lists/Y, "
            "trakt.tv/users/X/watchlist, trakt.tv/movies/trending, etc."
        )

    if m.group("media"):
        return {
            "type": "special",
            "media_type": m.group("media").lower(),
            "kind": m.group("kind").lower(),
        }

    username = m.group("user")
    if m.group("wl"):
        return {"type": "watchlist", "username": username}

    return {"type": "list", "username": username, "slug": m.group("slug")}


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _fetch_paginated(url: str, client_id: str, params: dict | None = None) -> list:
    """Fetch all pages of a Trakt paginated endpoint."""
    results = []
    page = 1
    while True:
        p = dict(params or {}, page=page, limit=100)
        resp = requests.get(url, headers=_headers(client_id), params=p, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        results.extend(data)
        total_pages = int(resp.headers.get("X-Pagination-Page-Count", 1))
        if page >= total_pages:
            break
        page += 1
    return results


# ── Fetchers ──────────────────────────────────────────────────────────────────

def fetch_list_info(client_id: str, username: str, slug: str) -> dict:
    url = f"{TRAKT_BASE}/users/{username}/lists/{slug}"
    resp = requests.get(url, headers=_headers(client_id), timeout=15)
    resp.raise_for_status()
    return resp.json()


def fetch_list_items(client_id: str, username: str, slug: str) -> list:
    url = f"{TRAKT_BASE}/users/{username}/lists/{slug}/items"
    return _fetch_paginated(url, client_id)


def fetch_watchlist(client_id: str, username: str) -> list:
    url = f"{TRAKT_BASE}/users/{username}/watchlist"
    return _fetch_paginated(url, client_id)


def fetch_special_list(client_id: str, media_type: str, kind: str) -> list:
    url = f"{TRAKT_BASE}/{media_type}/{kind}"
    return _fetch_paginated(url, client_id)


# ── Normalization ─────────────────────────────────────────────────────────────

def normalize_items(raw_items: list) -> list[dict]:
    """
    Convert raw Trakt API items to normalized dicts:
    {type, title, year, tmdb_id, imdb_id, trakt_id, slug}
    Only 'movie' and 'show' types are kept.
    """
    result = []
    for raw in raw_items:
        media_type = raw.get("type")
        if media_type not in ("movie", "show"):
            continue
        media = raw.get(media_type, {})
        ids = media.get("ids", {})
        result.append({
            "type":     media_type,
            "title":    media.get("title", ""),
            "year":     media.get("year"),
            "tmdb_id":  ids.get("tmdb"),
            "imdb_id":  ids.get("imdb"),
            "trakt_id": ids.get("trakt"),
            "slug":     ids.get("slug", ""),
        })
    return result


# ── Main entry point ──────────────────────────────────────────────────────────

def fetch_trakt_list(client_id: str, url: str) -> tuple[str, list[dict]]:
    """
    Fetch and normalize a Trakt list from any supported URL.
    Returns (list_name, normalized_items).
    Raises ValueError for bad URLs, requests.HTTPError for API errors.
    """
    parsed = parse_trakt_url(url)
    list_name = "Trakt List"

    if parsed["type"] == "list":
        try:
            info = fetch_list_info(client_id, parsed["username"], parsed["slug"])
            list_name = info.get("name", list_name)
        except Exception as exc:
            logger.warning("[TRAKT] Info liste indisponible: %s", exc)
        raw = fetch_list_items(client_id, parsed["username"], parsed["slug"])

    elif parsed["type"] == "watchlist":
        list_name = f"Watchlist — {parsed['username']}"
        raw = fetch_watchlist(client_id, parsed["username"])

    else:
        list_name = f"Trakt {parsed['media_type'].title()} {parsed['kind'].title()}"
        raw = fetch_special_list(client_id, parsed["media_type"], parsed["kind"])

    items = normalize_items(raw)
    logger.info("[TRAKT] '%s' — %d éléments récupérés", list_name, len(items))
    return list_name, items
