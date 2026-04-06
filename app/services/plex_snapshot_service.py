import json
import logging
import os
import re
from datetime import datetime

from plexapi.myplex import MyPlexAccount

from app.config_paths import CACHE_DIR
from app.services.plex_service import connect_to_server

logger = logging.getLogger(__name__)

SNAPSHOT_DIR = os.path.join(CACHE_DIR, "snapshots")


def _ensure_snapshot_dir():
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)


def _snapshot_path(filename):
    return os.path.join(SNAPSHOT_DIR, filename)


def create_snapshot(plex_token, label=""):
    """
    Connecte au serveur Plex local (owned=True), sérialise toutes les
    bibliothèques (films + séries + épisodes) et sauvegarde le snapshot.
    Retourne le nom du fichier créé.
    """
    account = MyPlexAccount(token=plex_token)
    my_server_res = next(
        (s for s in account.resources() if s.provides == "server" and getattr(s, "owned", False)),
        None,
    )
    if not my_server_res:
        raise RuntimeError("Serveur Plex local introuvable.")

    server = connect_to_server(my_server_res, plex_token, prefer_local=True)
    if not server:
        raise RuntimeError("Impossible de se connecter au serveur Plex local.")

    libraries = []
    for section in server.library.sections():
        items = []
        if section.type == "movie":
            for item in section.all():
                items.append({
                    "title": getattr(item, "title", "") or "",
                    "originalTitle": getattr(item, "originalTitle", "") or "",
                    "year": getattr(item, "year", None),
                    "guid": getattr(item, "guid", "") or "",
                    "type": "movie",
                })
        elif section.type == "show":
            for item in section.all():
                episodes = []
                try:
                    for ep in item.episodes():
                        episodes.append({
                            "season": getattr(ep, "seasonNumber", None),
                            "episode": getattr(ep, "index", None),
                            "title": getattr(ep, "title", "") or "",
                        })
                except Exception:
                    pass
                items.append({
                    "title": getattr(item, "title", "") or "",
                    "originalTitle": getattr(item, "originalTitle", "") or "",
                    "year": getattr(item, "year", None),
                    "guid": getattr(item, "guid", "") or "",
                    "type": "show",
                    "episodes": episodes,
                })
        else:
            # music / photo — titres seulement
            for item in section.all():
                items.append({
                    "title": getattr(item, "title", "") or "",
                    "year": getattr(item, "year", None),
                    "guid": getattr(item, "guid", "") or "",
                    "type": section.type,
                })

        libraries.append({
            "title": section.title,
            "type": section.type,
            "items": items,
        })
        logger.info("[SNAPSHOT] section '%s' — %d items", section.title, len(items))

    _ensure_snapshot_dir()
    now = datetime.now()
    filename = f"snapshot_{now.strftime('%Y-%m-%d_%H-%M-%S')}.json"
    payload = {
        "created_at": now.isoformat(),
        "label": (label or "").strip(),
        "server_name": getattr(my_server_res, "name", ""),
        "libraries": libraries,
    }
    path = _snapshot_path(filename)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)
    logger.info("[SNAPSHOT] Créé : %s (%d bibliothèques)", filename, len(libraries))
    return filename


def list_snapshots():
    """Retourne la liste des snapshots triée par date décroissante."""
    _ensure_snapshot_dir()
    result = []
    for fname in os.listdir(SNAPSHOT_DIR):
        if not fname.startswith("snapshot_") or not fname.endswith(".json"):
            continue
        path = _snapshot_path(fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            total_items = sum(len(lib.get("items", [])) for lib in data.get("libraries", []))
            result.append({
                "filename": fname,
                "created_at": data.get("created_at", ""),
                "label": data.get("label", ""),
                "server_name": data.get("server_name", ""),
                "total_items": total_items,
                "size_kb": round(os.path.getsize(path) / 1024),
            })
        except Exception:
            continue
    result.sort(key=lambda x: x["created_at"], reverse=True)
    return result


def load_snapshot(filename):
    """Charge un snapshot depuis son nom de fichier."""
    path = _snapshot_path(_safe_filename(filename))
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def delete_snapshot(filename):
    path = _snapshot_path(_safe_filename(filename))
    if os.path.exists(path):
        os.remove(path)
        return True
    return False


def _safe_filename(name):
    """Valide le nom de fichier pour éviter la traversée de répertoire."""
    name = os.path.basename(name)
    if not re.match(r'^snapshot_[\d_-]+\.json$', name):
        raise ValueError(f"Nom de snapshot invalide : {name}")
    return name


def _normalize(title):
    return "".join(ch.lower() for ch in (title or "") if ch.isalnum())


def _item_key(item):
    """Clé de matching : guid si dispo, sinon titre normalisé + année."""
    guid = (item.get("guid") or "").strip()
    if guid and not guid.startswith("local://"):
        return f"guid:{guid}"
    title = _normalize(item.get("title") or item.get("originalTitle") or "")
    year = item.get("year") or ""
    return f"title:{title}:{year}"


def diff_snapshots(snap_ref, snap_current):
    """
    Compare snap_ref (référence, plus ancien) vs snap_current (état actuel).
    Retourne un dict :
      {
        "missing_movies": [...],   # films présents dans ref mais absents dans current
        "missing_shows": [...],    # séries entièrement absentes
        "partial_shows": [...],    # séries présentes mais avec des épisodes manquants
        "new_movies": [...],       # films nouveaux dans current (pas dans ref)
        "new_shows": [...],        # séries nouvelles dans current
      }
    """
    def _index_by_type(snap, media_type):
        index = {}
        for lib in snap.get("libraries", []):
            if lib.get("type") != media_type:
                continue
            for item in lib.get("items", []):
                key = _item_key(item)
                index[key] = item
        return index

    ref_movies = _index_by_type(snap_ref, "movie")
    cur_movies = _index_by_type(snap_current, "movie")
    ref_shows = _index_by_type(snap_ref, "show")
    cur_shows = _index_by_type(snap_current, "show")

    # Films manquants / nouveaux
    missing_movies = [ref_movies[k] for k in ref_movies if k not in cur_movies]
    new_movies = [cur_movies[k] for k in cur_movies if k not in ref_movies]

    # Séries : manquantes entièrement ou partiellement
    missing_shows = []
    partial_shows = []
    new_shows = [cur_shows[k] for k in cur_shows if k not in ref_shows]

    for key, ref_show in ref_shows.items():
        if key not in cur_shows:
            missing_shows.append(ref_show)
            continue

        cur_show = cur_shows[key]
        ref_eps = {(ep.get("season"), ep.get("episode")) for ep in ref_show.get("episodes", [])}
        cur_eps = {(ep.get("season"), ep.get("episode")) for ep in cur_show.get("episodes", [])}

        missing_eps = sorted(ref_eps - cur_eps, key=lambda x: (x[0] or 0, x[1] or 0))
        if missing_eps:
            partial_shows.append({
                "title": ref_show.get("title"),
                "year": ref_show.get("year"),
                "guid": ref_show.get("guid"),
                "missing_episodes": [
                    {"season": s, "episode": e} for s, e in missing_eps
                ],
            })

    # Trier par titre
    def _sort_title(x):
        return _normalize(x.get("title") or "")

    missing_movies.sort(key=_sort_title)
    new_movies.sort(key=_sort_title)
    missing_shows.sort(key=_sort_title)
    partial_shows.sort(key=_sort_title)
    new_shows.sort(key=_sort_title)

    return {
        "missing_movies": missing_movies,
        "missing_shows": missing_shows,
        "partial_shows": partial_shows,
        "new_movies": new_movies,
        "new_shows": new_shows,
    }
