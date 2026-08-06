"""
playlist_service.py — Matching Trakt→Plex et création de playlists Plex.

Flux :
  1. Connexion au serveur Plex local (owned=True).
  2. Construction d'un index GUID → item depuis toutes les biblio films/séries
     (une seule passe, fiable pour l'agent Plex moderne plex://show/…).
  3. Matching de chaque item Trakt via l'index GUID, puis fallback titre+année.
  4. Création (ou recréation) de la playlist Plex.
"""
import logging
import re

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


# ── Index GUID (scan unique par session de matching) ─────────────────────────

def _build_guid_index(plex, section_types: set | None = None) -> dict:
    """
    Scanne les sections films/séries et construit un dict
    {guid_string: plex_item} en lisant les GUIDs externes de chaque item
    (attribut .guids — liste de Guid(id='tvdb://…') sur l'agent Plex moderne).

    Un seul scan par session ; toutes les lookups GUID sont ensuite O(1).

    section_types : set de types à scanner, ex. {"movie"}, {"show"} ou None
                    pour scanner les deux. Permet d'éviter de scanner toute la
                    bibliothèque séries quand on cherche seulement des films.
    """
    if section_types is None:
        section_types = {"show", "movie"}

    index: dict = {}
    for section in plex.library.sections():
        if section.type not in section_types:
            continue
        try:
            for item in section.all():
                # GUIDs externes (tvdb, tmdb, imdb, …) — agent Plex moderne
                for g in getattr(item, "guids", []):
                    gid = getattr(g, "id", None)
                    if gid:
                        index.setdefault(gid, item)
                # GUID principal (peut être plex:// ou tvdb:// selon l'agent)
                primary = getattr(item, "guid", None)
                if primary:
                    index.setdefault(primary, item)
        except Exception as exc:
            logger.warning("[PLAYLIST] Erreur scan section '%s': %s", section.title, exc)

    logger.info("[PLAYLIST] Index GUID: %d entrées (sections: %s)",
                len(index), ", ".join(sorted(section_types)))
    return index


def _guid_lookup(index: dict, tmdb_id, imdb_id, tvdb_id):
    """Cherche dans l'index GUID : TMDB → IMDB → TVDB. Retourne le premier match."""
    candidates = []
    if tmdb_id:
        candidates.append(f"tmdb://{tmdb_id}")
    if imdb_id:
        candidates.append(f"imdb://{imdb_id}")
    if tvdb_id:
        candidates.append(f"tvdb://{tvdb_id}")
    for guid in candidates:
        item = index.get(guid)
        if item:
            logger.debug("[PLAYLIST] GUID match '%s' via %s", item.title, guid)
            return item
    return None


# ── Matching par titre (fallback) ─────────────────────────────────────────────

_VOL_RE  = re.compile(r'\bvolume\b', re.IGNORECASE)
_THE_RE  = re.compile(r'^(the|les?|l\')\s+', re.IGNORECASE)
_DIG2WRD = {'1': 'One', '2': 'Two', '3': 'Three', '4': 'Four',
             '5': 'Five', '6': 'Six', '7': 'Seven', '8': 'Eight', '9': 'Nine'}
_WRD2DIG = {v.lower(): k for k, v in _DIG2WRD.items()}
_DIG_RE  = re.compile(r'\b([1-9])\b')
_WRD_RE  = re.compile(r'\b(one|two|three|four|five|six|seven|eight|nine)\b', re.IGNORECASE)


def _title_variants(title: str) -> list[str]:
    t0 = title.strip()

    def _apply(t):
        yield t
        t1 = _VOL_RE.sub("Vol.", t)
        if t1 != t:
            yield t1
        t2 = _DIG_RE.sub(lambda m: _DIG2WRD[m.group(1)], t)
        if t2 != t:
            yield t2
        t3 = _WRD_RE.sub(lambda m: _WRD2DIG[m.group(1).lower()], t)
        if t3 != t:
            yield t3

    seen: set[str] = set()
    result = []
    for base in [t0, _THE_RE.sub("", t0).strip()]:
        for v in _apply(base):
            if v and v not in seen:
                seen.add(v)
                result.append(v)
    return result


def _find_by_title(plex, title: str, year, media_type: str):
    """
    Fallback titre : cherche par title ET originalTitle, avec variantes.
    Filtre par année si disponible.
    """
    libtype = "movie" if media_type == "movie" else "show"
    year_str = str(year or "")
    variants = _title_variants(title)

    for variant in variants:
        for field in ("title", "originalTitle"):
            try:
                results = plex.library.search(libtype=libtype, **{field: variant})
            except Exception:
                continue
            if not results:
                continue
            if year_str:
                for r in results:
                    if str(getattr(r, "year", "")) == year_str:
                        return r
            return results[0]

    return None


# ── Helpers show parent ───────────────────────────────────────────────────────

def _find_show(plex, item: dict, guid_index: dict):
    """Trouve la série parente via l'index GUID, puis fallback titre."""
    show = _guid_lookup(
        guid_index,
        tmdb_id=item.get("show_tmdb_id"),
        imdb_id=item.get("show_imdb_id"),
        tvdb_id=item.get("show_tvdb_id"),
    )
    if not show:
        show = _find_by_title(plex, item.get("show_title", ""), item.get("year"), "show")
    return show


# ── Finders épisode / saison ──────────────────────────────────────────────────

def _find_season(plex, item: dict, guid_index: dict):
    season_num = item.get("season")
    if season_num is None:
        return None

    show = _find_show(plex, item, guid_index)
    if not show:
        return None

    try:
        return show.season(season=season_num)
    except Exception:
        pass
    try:
        for s in show.seasons():
            if s.index == season_num:
                return s
    except Exception:
        pass
    return None


def _find_episode(plex, item: dict, guid_index: dict):
    season = item.get("season")
    number = item.get("episode")
    if season is None or number is None:
        return None

    show = _find_show(plex, item, guid_index)
    if not show:
        return None

    try:
        return show.episode(season=season, episode=number)
    except Exception:
        pass
    try:
        for ep in show.episodes():
            if ep.seasonNumber == season and ep.index == number:
                return ep
    except Exception:
        pass
    return None


# ── Matching principal ────────────────────────────────────────────────────────

def match_trakt_items(plex, trakt_items: list) -> dict:
    """
    Match a list of normalized Trakt items against the Plex library.

    Construit d'abord un index GUID complet (un seul scan des bibliothèques),
    puis matche chaque item via l'index (O(1)) avec fallback titre+année.

    Returns:
        {
            "matched":   [{"trakt": …, "plex": plex_item}, …],
            "unmatched": [trakt_item, …],
        }
    """
    guid_index = _build_guid_index(plex)

    matched   = []
    unmatched = []

    for item in trakt_items:
        plex_item  = None
        media_type = item.get("type", "movie")

        if media_type == "episode":
            plex_item = _find_episode(plex, item, guid_index)

        elif media_type == "season":
            plex_item = _find_season(plex, item, guid_index)

        else:
            # Films et séries : GUID d'abord, titre+année en fallback
            plex_item = _guid_lookup(
                guid_index,
                tmdb_id=item.get("tmdb_id"),
                imdb_id=item.get("imdb_id"),
                tvdb_id=item.get("tvdb_id"),
            )
            if not plex_item:
                plex_item = _find_by_title(
                    plex,
                    title=item.get("title") or "",
                    year=item.get("year"),
                    media_type=media_type,
                )

        if plex_item:
            matched.append({"trakt": item, "plex": plex_item})
        else:
            unmatched.append(item)

    logger.info("[PLAYLIST] Matching — %d/%d trouvés", len(matched), len(trakt_items))
    return {"matched": matched, "unmatched": unmatched}


# ── Création de playlist ──────────────────────────────────────────────────────

def create_or_update_playlist(plex, name: str, plex_items: list):
    """
    Supprime la playlist existante du même nom (si présente) et en crée une nouvelle.
    Retourne l'objet playlist créé, ou None si plex_items est vide.
    """
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
    """
    plex   = _get_local_server(plex_token)
    report = match_trakt_items(plex, trakt_items)

    plex_items = [m["plex"] for m in report["matched"]]
    playlist   = create_or_update_playlist(plex, playlist_name, plex_items)

    report["playlist_name"]    = playlist_name
    report["playlist_key"]     = str(getattr(playlist, "ratingKey", "") or "") or None
    report["matched_count"]    = len(report["matched"])
    report["unmatched_count"]  = len(report["unmatched"])
    return report
