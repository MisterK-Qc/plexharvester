"""
Service de gestion des alias de titres FTP.

Fichier : cache/ftp_title_aliases.json
Structure :
{
  "Chair de poule": {"ftp_title": "Goosebumps", "source": "manual", "media_type": "show"},
  "Les Simpsons":   {"ftp_title": "The Simpsons", "source": "tmdb",   "media_type": "show"}
}

Priorité : manual > tmdb
- Les entrées "manual" ne sont jamais écrasées par une recherche TMDB.
- Les entrées "tmdb" sont créées automatiquement et peuvent être promues en "manual"
  depuis la page Config.
"""

import json
import logging
import os
import threading

import requests

from app.config_paths import CACHE_DIR

logger = logging.getLogger(__name__)

ALIAS_FILE = os.path.join(CACHE_DIR, "ftp_title_aliases.json")
_lock = threading.Lock()

# Cache mémoire des alias — évite de relire le fichier à chaque appel
_alias_cache: dict | None = None
_alias_mtime: float | None = None


# ─────────────────────────────────────────────
# Chargement / sauvegarde
# ─────────────────────────────────────────────

def load_aliases() -> dict:
    global _alias_cache, _alias_mtime
    try:
        mtime = os.path.getmtime(ALIAS_FILE) if os.path.exists(ALIAS_FILE) else None
    except OSError:
        mtime = None

    with _lock:
        if _alias_cache is not None and _alias_mtime == mtime:
            return dict(_alias_cache)

    # Recharger depuis le disque
    try:
        if os.path.exists(ALIAS_FILE):
            with open(ALIAS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {}
    except Exception:
        data = {}

    with _lock:
        _alias_cache = data
        _alias_mtime = mtime

    return dict(data)


def save_aliases(aliases: dict) -> None:
    global _alias_cache, _alias_mtime
    with _lock:
        with open(ALIAS_FILE, "w", encoding="utf-8") as f:
            json.dump(aliases, f, indent=2, ensure_ascii=False)
        _alias_cache = dict(aliases)
        try:
            _alias_mtime = os.path.getmtime(ALIAS_FILE)
        except OSError:
            _alias_mtime = None


# ─────────────────────────────────────────────
# Lecture d'un alias
# ─────────────────────────────────────────────

def get_alias(title: str) -> str | None:
    """Retourne le titre FTP associé, ou None si absent."""
    if not title:
        return None
    aliases = load_aliases()
    entry = aliases.get(title) or aliases.get(title.lower())
    return entry["ftp_title"] if entry else None


# ─────────────────────────────────────────────
# Lookup TMDB → original_title / original_name
# ─────────────────────────────────────────────

def _title_similarity(a: str, b: str) -> float:
    """
    Similarité simple entre deux titres : ratio de tokens communs.
    Retourne une valeur entre 0.0 et 1.0.
    """
    def tokens(s):
        s = s.lower().strip()
        # Retirer accents, ponctuation
        import unicodedata, re
        s = unicodedata.normalize("NFKD", s)
        s = "".join(c for c in s if not unicodedata.combining(c))
        s = re.sub(r"[^a-z0-9\s]", " ", s)
        return set(s.split())

    ta, tb = tokens(a), tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(len(ta), len(tb))


def _tmdb_original_title(title: str, media_type: str, api_key: str) -> str | None:
    """
    Interroge TMDB pour obtenir le titre original (EN) d'un titre FR.
    media_type : "tv" ou "movie"
    Retourne le titre original ou None.

    Garde uniquement le résultat si le titre TMDB (FR ou EN) ressemble
    suffisamment au titre cherché — évite les faux positifs.
    """
    try:
        url = f"https://api.themoviedb.org/3/search/{media_type}"
        r = requests.get(url, params={
            "api_key": api_key,
            "query": title,
            "language": "fr-CA",
        }, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])
        if not results:
            return None

        first = results[0]
        orig = first.get("original_name") or first.get("original_title") or ""
        orig = orig.strip()
        tmdb_fr = (first.get("name") or first.get("title") or "").strip()

        if not orig:
            return None

        # Cas EN→FR : l'original TMDB est identique au titre cherché (ex: "The Book Thief")
        # → utiliser le titre localisé FR à la place (ex: "La Voleuse de livres")
        if orig.lower() == title.lower():
            if tmdb_fr and tmdb_fr.lower() != title.lower():
                logger.debug(
                    "[FTP_ALIAS] TMDB EN→FR: '%s' → '%s' (titre localisé FR)",
                    title, tmdb_fr
                )
                return tmdb_fr
            return None

        # Cas FR→EN : vérifier que le résultat ressemble suffisamment au titre cherché
        sim_fr = _title_similarity(title, tmdb_fr) if tmdb_fr else 0.0
        sim_orig = _title_similarity(title, orig)
        if max(sim_fr, sim_orig) < 0.4:
            logger.debug(
                "[FTP_ALIAS] TMDB résultat rejeté pour '%s' → '%s' (sim_fr=%.2f sim_orig=%.2f)",
                title, orig, sim_fr, sim_orig
            )
            return None

        return orig

    except Exception as e:
        logger.debug("[FTP_ALIAS] TMDB lookup échoué pour '%s': %s", title, e)
        return None


# ─────────────────────────────────────────────
# Lookup TVMaze → original_title (séries seulement, sans clé)
# ─────────────────────────────────────────────

def _tvmaze_original_title(title: str) -> str | None:
    """
    Interroge TVMaze (gratuit, sans clé) pour obtenir le titre original EN d'un titre FR.
    Séries uniquement. Retourne le titre original ou None.
    """
    try:
        r = requests.get(
            "https://api.tvmaze.com/singlesearch/shows",
            params={"q": title},
            timeout=8,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json()

        orig = (data.get("name") or "").strip()
        if not orig:
            return None

        # Rejeter si le résultat ne ressemble pas du tout au titre cherché
        if _title_similarity(title, orig) < 0.35:
            logger.debug(
                "[FTP_ALIAS] TVMaze résultat rejeté pour '%s' → '%s' (similarité trop faible)",
                title, orig,
            )
            return None

        # Si le titre trouvé est identique (déjà EN), pas besoin d'alias
        if orig.lower() == title.lower():
            return None

        return orig

    except Exception as e:
        logger.debug("[FTP_ALIAS] TVMaze lookup échoué pour '%s': %s", title, e)
        return None


# ─────────────────────────────────────────────
# Résolution avec fallback TMDB puis TVMaze
# ─────────────────────────────────────────────

def resolve_alias(title: str, media_type: str = "tv", api_key: str = "") -> str | None:
    """
    Retourne le titre FTP à utiliser pour `title`.

    1. Cherche dans les alias existants (manual prioritaire, puis tmdb/tvmaze).
    2. Si absent et TMDB configuré, interroge TMDB.
    3. Si TMDB absent ou sans résultat et media_type=="tv", essaie TVMaze (sans clé).
    4. Sauvegarde le résultat et retourne None si aucun alias trouvé.

    media_type : "tv" (séries) ou "movie" (films)
    """
    if not title:
        return None

    aliases = load_aliases()
    entry = aliases.get(title) or aliases.get(title.lower())
    if entry:
        return entry["ftp_title"]

    orig = None
    source = None

    # Essayer TMDB si configuré
    if api_key:
        orig = _tmdb_original_title(title, media_type, api_key)
        if orig:
            source = "tmdb"

    # Fallback TVMaze pour les séries (gratuit, sans clé)
    if orig is None and media_type == "tv":
        orig = _tvmaze_original_title(title)
        if orig:
            source = "tvmaze"

    if not orig:
        return None

    logger.info("[FTP_ALIAS] %s auto-alias: '%s' → '%s' (%s)", source.upper(), title, orig, media_type)

    aliases[title] = {
        "ftp_title": orig,
        "source": source,
        "media_type": media_type,
    }
    save_aliases(aliases)

    return orig


# ─────────────────────────────────────────────
# Gestion depuis la page Config
# ─────────────────────────────────────────────

def upsert_manual_alias(plex_title: str, ftp_title: str, media_type: str = "") -> None:
    """Ajoute ou met à jour un alias manuel."""
    aliases = load_aliases()
    aliases[plex_title] = {
        "ftp_title": ftp_title,
        "source": "manual",
        "media_type": media_type,
    }
    save_aliases(aliases)


def promote_alias(plex_title: str) -> bool:
    """Promeut un alias tmdb en manuel. Retourne True si trouvé."""
    aliases = load_aliases()
    if plex_title in aliases:
        aliases[plex_title]["source"] = "manual"
        save_aliases(aliases)
        return True
    return False


def delete_alias(plex_title: str) -> bool:
    """Supprime un alias. Retourne True si trouvé."""
    aliases = load_aliases()
    if plex_title in aliases:
        del aliases[plex_title]
        save_aliases(aliases)
        return True
    return False


def save_manual_aliases_from_form(pairs: list[tuple[str, str]]) -> None:
    """
    Reçoit une liste de (plex_title, ftp_title) depuis le formulaire Config.
    Met à jour uniquement les entrées "manual" — les entrées "tmdb" sont préservées.
    """
    aliases = load_aliases()

    # Supprimer les anciennes entrées manuelles
    to_delete = [k for k, v in aliases.items() if v.get("source") == "manual"]
    for k in to_delete:
        del aliases[k]

    # Ajouter les nouvelles
    for plex_title, ftp_title in pairs:
        plex_title = plex_title.strip()
        ftp_title = ftp_title.strip()
        if plex_title and ftp_title:
            aliases[plex_title] = {
                "ftp_title": ftp_title,
                "source": "manual",
                "media_type": "",
            }

    save_aliases(aliases)
