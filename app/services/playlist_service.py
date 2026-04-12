"""
playlist_service.py — Matching Trakt→Plex et création de playlists Plex.

Flux :
  1. Connexion au serveur Plex local (owned=True).
  2. Construction d'un index TMDB→item depuis toutes les biblio films/séries.
  3. Matching de chaque item Trakt (TMDB ID d'abord, titre+année en fallback).
  4. Création (ou recréation) de la playlist Plex.
"""
import logging

from plexapi.myplex import MyPlexAccount

from .plex_service import normalize_name, connect_to_server

logger = logging.getLogger(__name__)


# ── Connexion au serveur local ────────────────────────────────────────────────

def _get_local_server(plex_token: str):
    account = MyPlexAccount(token=plex_token)
    resources = account.resources()
    local = next(
        (r for r in resources
         if getattr(r, "owned", False) and "server" in (getattr(r, "provides", "") or "")),
        None,
    )
    if not local:
        raise RuntimeError("Serveur Plex local (owned=True) introuvable")
    server = connect_to_server(local, plex_token)
    if not server:
        raise RuntimeError(f"Impossible de se connecter à '{local.name}'")
    return server


# ── Index TMDB ────────────────────────────────────────────────────────────────

def _build_plex_indexes(plex):
    """
    Parcourt toutes les bibliothèques film/série et construit :
      tmdb_index        : {tmdb_id (int) → plex_item}
      title_year_index  : {(titre_normalisé, année_str) → plex_item}
    """
    tmdb_index: dict[int, object] = {}
    title_year_index: dict[tuple, object] = {}

    for section in plex.library.sections():
        if section.type not in ("movie", "show"):
            continue
        try:
            items = section.all()
        except Exception as exc:
            logger.warning("[PLAYLIST] Erreur chargement biblio '%s': %s", section.title, exc)
            continue

        for item in items:
            # --- Index TMDB via .guids (Plex 1.20+) ---
            try:
                for guid in item.guids:
                    gid = guid.id or ""
                    if gid.startswith("tmdb://"):
                        try:
                            tmdb_id = int(gid.replace("tmdb://", "").split("?")[0])
                            tmdb_index.setdefault(tmdb_id, item)
                        except ValueError:
                            pass
            except Exception:
                pass

            # --- Index titre+année (fallback) ---
            title = (getattr(item, "title", "") or "").strip()
            orig  = (getattr(item, "originalTitle", "") or "").strip()
            year  = str(getattr(item, "year", "") or "")

            for t in {title, orig}:
                if not t:
                    continue
                for key in [(t.lower(), year), (normalize_name(t), year),
                             (t.lower(), ""),  (normalize_name(t), "")]:
                    title_year_index.setdefault(key, item)

    logger.info(
        "[PLAYLIST] Index construit — %d TMDB, %d titre/année",
        len(tmdb_index), len(title_year_index),
    )
    return tmdb_index, title_year_index


# ── Matching ──────────────────────────────────────────────────────────────────

def match_trakt_items(plex, trakt_items: list) -> dict:
    """
    Match a list of normalized Trakt items against the Plex library.

    Returns:
        {
            "matched":   [{"trakt": ..., "plex": plex_item}, ...],
            "unmatched": [trakt_item, ...],
        }
    """
    tmdb_index, title_year_index = _build_plex_indexes(plex)
    matched = []
    unmatched = []

    for item in trakt_items:
        plex_item = None

        # 1. TMDB ID
        tmdb_id = item.get("tmdb_id")
        if tmdb_id:
            plex_item = tmdb_index.get(int(tmdb_id))

        # 2. Titre + année (fallback)
        if not plex_item:
            title = (item.get("title") or "").strip()
            year  = str(item.get("year") or "")
            norm  = normalize_name(title)
            plex_item = (
                title_year_index.get((title.lower(), year))
                or title_year_index.get((norm, year))
                or title_year_index.get((title.lower(), ""))
                or title_year_index.get((norm, ""))
            )

        if plex_item:
            matched.append({"trakt": item, "plex": plex_item})
        else:
            unmatched.append(item)

    logger.info(
        "[PLAYLIST] Matching — %d/%d trouvés",
        len(matched), len(trakt_items),
    )
    return {"matched": matched, "unmatched": unmatched}


# ── Création de playlist ──────────────────────────────────────────────────────

def create_or_update_playlist(plex, name: str, plex_items: list):
    """
    Supprime la playlist existante du même nom (si présente) et en crée une nouvelle.
    Retourne l'objet playlist créé, ou None si plex_items est vide.
    """
    # Supprimer l'ancienne version
    try:
        existing = plex.playlist(name)
        existing.delete()
        logger.info("[PLAYLIST] Playlist '%s' supprimée pour recréation", name)
    except Exception:
        pass  # n'existait pas

    if not plex_items:
        logger.warning("[PLAYLIST] Aucun élément à ajouter — playlist '%s' non créée", name)
        return None

    playlist = plex.createPlaylist(name, items=plex_items)
    logger.info("[PLAYLIST] Playlist '%s' créée avec %d éléments", name, len(plex_items))
    return playlist


# ── Point d'entrée principal ──────────────────────────────────────────────────

def import_trakt_to_plex(plex_token: str, playlist_name: str, trakt_items: list) -> dict:
    """
    Pipeline complet : matching Trakt→Plex + création playlist.

    Returns un rapport :
        {
            "matched":        [{trakt, plex}, ...],
            "unmatched":      [trakt_item, ...],
            "matched_count":  int,
            "unmatched_count": int,
            "playlist_name":  str,
            "playlist_key":   str | None,
        }
    """
    plex   = _get_local_server(plex_token)
    report = match_trakt_items(plex, trakt_items)

    plex_items = [m["plex"] for m in report["matched"]]
    playlist   = create_or_update_playlist(plex, playlist_name, plex_items)

    report["playlist_name"]   = playlist_name
    report["playlist_key"]    = str(getattr(playlist, "ratingKey", "") or "") or None
    report["matched_count"]   = len(report["matched"])
    report["unmatched_count"] = len(report["unmatched"])
    return report
