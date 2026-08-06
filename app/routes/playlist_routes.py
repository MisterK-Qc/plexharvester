from flask import Blueprint, render_template, request, session, redirect, url_for, jsonify, current_app

from ..services.config_service import load_config
from ..services.trakt_service import fetch_trakt_list
from ..services.playlist_service import (
    _get_local_server, _build_guid_index, _guid_lookup,
    _find_by_title, _find_episode, _find_season,
    match_trakt_items, create_or_update_playlist,
)
from ..services.playlist_store_service import (
    list_playlists, upsert_playlist, get_playlist, delete_playlist,
)

playlist_bp = Blueprint("playlist", __name__, url_prefix="/playlists")


def _resolve_client_id(data: dict) -> str:
    cid = (data.get("client_id") or "").strip()
    if not cid:
        cid = load_config().get("TRAKT_CLIENT_ID", "").strip()
    return cid


@playlist_bp.route("/")
def playlists_page():
    if not session.get("logged_in"):
        return redirect(url_for("auth.login"))
    cfg = load_config()
    return render_template(
        "playlists.html",
        trakt_client_id=cfg.get("TRAKT_CLIENT_ID", ""),
    )


@playlist_bp.route("/preview", methods=["POST"])
def preview_playlist():
    """Fetch Trakt list + match against Plex — sans créer la playlist."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401

    plex_token = session.get("plex_token")
    if not plex_token:
        return jsonify({"ok": False, "error": "Token Plex manquant"}), 401

    data      = request.get_json(silent=True) or {}
    trakt_url = (data.get("trakt_url") or "").strip()
    client_id = _resolve_client_id(data)

    if not client_id:
        return jsonify({"ok": False, "error": "Trakt Client ID manquant — configure-le dans Config"}), 400
    if not trakt_url:
        return jsonify({"ok": False, "error": "URL Trakt manquante"}), 400

    try:
        list_name, trakt_items = fetch_trakt_list(client_id, trakt_url)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        current_app.logger.exception("[PLAYLIST] Erreur fetch Trakt")
        return jsonify({"ok": False, "error": f"Erreur Trakt : {exc}"}), 500

    if not trakt_items:
        return jsonify({
            "ok": True, "list_name": list_name,
            "total": 0, "matched": 0, "unmatched": 0,
            "matched_items": [], "unmatched_items": [],
        })

    try:
        plex   = _get_local_server(plex_token)
        report = match_trakt_items(plex, trakt_items)
    except Exception as exc:
        current_app.logger.exception("[PLAYLIST] Erreur matching Plex")
        return jsonify({"ok": False, "error": f"Erreur Plex : {exc}"}), 500

    # Reconstruire l'ordre original Trakt avec matched/unmatched mélangés
    # Clé prioritaire : trakt_id (unique), fallback tmdb_id, fallback title+year
    matched_by_trakt   = {m["trakt"].get("trakt_id"): m for m in report["matched"] if m["trakt"].get("trakt_id")}
    matched_by_tmdb    = {m["trakt"].get("tmdb_id"):  m for m in report["matched"] if m["trakt"].get("tmdb_id")}
    matched_by_ty      = {(m["trakt"]["title"], m["trakt"].get("year")): m for m in report["matched"]}

    ordered_items = []
    for item in trakt_items:
        m = (matched_by_trakt.get(item.get("trakt_id")) or
             matched_by_tmdb.get(item.get("tmdb_id")) or
             matched_by_ty.get((item["title"], item.get("year"))))
        plex_obj = m["plex"] if m else None

        entry = {
            "title":      item["title"],
            "year":       item["year"],
            "type":       item["type"],
            "tmdb_id":    item.get("tmdb_id"),
            "imdb_id":    item.get("imdb_id"),
            "tvdb_id":    item.get("tvdb_id"),
            "matched":    plex_obj is not None,
            "plex_key":   str(plex_obj.ratingKey) if plex_obj else None,
            "plex_title": plex_obj.title           if plex_obj else None,
        }

        # Champs spécifiques aux épisodes et saisons
        if item["type"] in ("episode", "season"):
            entry["show_title"]   = item.get("show_title")
            entry["show_tmdb_id"] = item.get("show_tmdb_id")
            entry["show_tvdb_id"] = item.get("show_tvdb_id")
            entry["show_imdb_id"] = item.get("show_imdb_id")
            entry["season"]       = item.get("season")
            entry["episode_num"]  = None
            if item["type"] == "episode":
                entry["episode_num"] = getattr(plex_obj, "index", None) or item.get("episode")
                if plex_obj:
                    entry["plex_title"] = getattr(plex_obj, "grandparentTitle", "") or item.get("show_title", "")
            elif item["type"] == "season" and plex_obj:
                # Nombre d'épisodes dans la saison
                try:
                    entry["episode_count"] = plex_obj.leafCount
                except Exception:
                    entry["episode_count"] = None
                entry["plex_title"] = getattr(plex_obj, "parentTitle", "") or item.get("show_title", "")

        ordered_items.append(entry)

    return jsonify({
        "ok":          True,
        "list_name":   list_name,
        "total":       len(trakt_items),
        "matched":     len(report["matched"]),
        "unmatched":   len(report["unmatched"]),
        "ordered_items": ordered_items,
        # Gardé pour l'onglet Introuvables
        "unmatched_items": [
            {
                "title":      u["title"],
                "year":       u["year"],
                "type":       u["type"],
                "tmdb_id":    u.get("tmdb_id"),
                "imdb_id":    u.get("imdb_id"),
                "tvdb_id":    u.get("tvdb_id"),
                "show_title": u.get("show_title") if u["type"] in ("episode", "season") else None,
                "season":     u.get("season")     if u["type"] in ("episode", "season") else None,
                "episode":    u.get("episode")    if u["type"] == "episode" else None,
            }
            for u in report["unmatched"]
        ],
    })


@playlist_bp.route("/push", methods=["POST"])
def push_playlist():
    """Crée (ou recrée) une playlist Plex à partir d'une liste de rating keys."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401

    plex_token = session.get("plex_token")
    if not plex_token:
        return jsonify({"ok": False, "error": "Token Plex manquant"}), 401

    data          = request.get_json(silent=True) or {}
    playlist_name = (data.get("playlist_name") or "").strip()
    plex_keys     = data.get("plex_keys") or []

    if not playlist_name:
        return jsonify({"ok": False, "error": "Nom de playlist manquant"}), 400
    if not plex_keys:
        return jsonify({"ok": False, "error": "Aucun item à ajouter"}), 400

    try:
        plex  = _get_local_server(plex_token)
        # Expand seasons to individual episodes to maintain correct playlist order
        items = []
        for k in plex_keys:
            obj = plex.fetchItem(int(k))
            if obj.type == "season":
                items.extend(obj.episodes())
            else:
                items.append(obj)
        create_or_update_playlist(plex, playlist_name, items)
    except Exception as exc:
        current_app.logger.exception("[PLAYLIST] Erreur push Plex")
        return jsonify({"ok": False, "error": f"Erreur Plex : {exc}"}), 500

    return jsonify({"ok": True, "playlist_name": playlist_name, "count": len(items)})


@playlist_bp.route("/plex-episodes/<int:plex_key>")
def plex_episodes(plex_key):
    """Retourne les épisodes d'une série Plex."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401
    plex_token = session.get("plex_token")
    try:
        plex = _get_local_server(plex_token)
        show = plex.fetchItem(plex_key)
        episodes = show.episodes()
        return jsonify({
            "ok": True,
            "episodes": [
                {
                    "key":     str(ep.ratingKey),
                    "season":  ep.seasonNumber,
                    "episode": ep.index,
                    "title":   ep.title,
                }
                for ep in episodes
            ],
        })
    except Exception as exc:
        current_app.logger.exception("[PLAYLIST] Erreur fetch épisodes")
        return jsonify({"ok": False, "error": str(exc)}), 500


@playlist_bp.route("/plex-search")
def plex_search():
    """Recherche dans le Plex local (pour ajout manuel)."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401
    plex_token = session.get("plex_token")
    q       = (request.args.get("q") or "").strip()
    libtype = (request.args.get("type") or "").strip()
    if not q:
        return jsonify({"ok": True, "results": []})
    try:
        plex = _get_local_server(plex_token)
        kwargs = {"title": q}
        if libtype in ("movie", "show"):
            kwargs["libtype"] = libtype
        results = plex.library.search(**kwargs)[:20]
        return jsonify({
            "ok": True,
            "results": [
                {
                    "key":   str(r.ratingKey),
                    "title": r.title,
                    "year":  getattr(r, "year", None),
                    "type":  r.type,
                }
                for r in results
            ],
        })
    except Exception as exc:
        current_app.logger.exception("[PLAYLIST] Erreur plex-search")
        return jsonify({"ok": False, "error": str(exc)}), 500


# ── Sauvegarde / chargement des playlists ────────────────────────────────────

@playlist_bp.route("/saved", methods=["GET"])
def saved_playlists():
    """Liste les playlists sauvegardées (résumés)."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401
    return jsonify({"ok": True, "playlists": list_playlists()})


@playlist_bp.route("/save", methods=["POST"])
def save_playlist():
    """Sauvegarde (ou met à jour) une définition de playlist."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401

    data  = request.get_json(silent=True) or {}
    name  = (data.get("name") or "").strip()
    items = data.get("items") or []

    if not name:
        return jsonify({"ok": False, "error": "Nom de playlist manquant"}), 400

    try:
        pid = upsert_playlist(name, data.get("trakt_url", ""), items)
    except Exception as exc:
        current_app.logger.exception("[PLAYLIST] Erreur save")
        return jsonify({"ok": False, "error": str(exc)}), 500

    return jsonify({"ok": True, "id": pid, "name": name})


@playlist_bp.route("/load/<pid>", methods=["GET"])
def load_playlist(pid):
    """
    Charge une playlist sauvegardée et re-vérifie la disponibilité Plex
    des items marqués comme non disponibles (placeholders).
    """
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401

    playlist = get_playlist(pid)
    if not playlist:
        return jsonify({"ok": False, "error": "Playlist introuvable"}), 404

    plex_token = session.get("plex_token")
    items = [dict(it) for it in playlist.get("items", [])]
    newly_available = 0

    placeholders = [it for it in items if not it.get("plex_key")]
    if placeholders and plex_token:
        try:
            plex        = _get_local_server(plex_token)
            # Ne scanner que les sections pertinentes pour les placeholders
            ph_types    = {it.get("type", "movie") for it in placeholders}
            sec_types   = set()
            if ph_types & {"movie"}:
                sec_types.add("movie")
            if ph_types & {"show", "episode", "season"}:
                sec_types.add("show")
            guid_index  = _build_guid_index(plex, sec_types or None)

            for it in items:
                if it.get("plex_key"):
                    continue
                media_type = it.get("type", "movie")
                plex_item  = None

                if media_type == "episode":
                    plex_item = _find_episode(plex, it, guid_index)
                elif media_type == "season":
                    plex_item = _find_season(plex, it, guid_index)
                else:
                    plex_item = _guid_lookup(
                        guid_index,
                        tmdb_id=it.get("tmdb_id"),
                        imdb_id=it.get("imdb_id"),
                        tvdb_id=it.get("tvdb_id"),
                    )
                    if not plex_item:
                        plex_item = _find_by_title(
                            plex,
                            title=it.get("title", ""),
                            year=it.get("year"),
                            media_type=media_type,
                        )

                if plex_item:
                    it["plex_key"]   = str(plex_item.ratingKey)
                    it["plex_title"] = plex_item.title
                    it["available"]  = True
                    newly_available += 1

        except Exception:
            current_app.logger.exception("[PLAYLIST] Erreur re-check load")

    auto_pushed = False
    if newly_available:
        upsert_playlist(playlist["name"], playlist.get("trakt_url", ""), items)
        # Push automatique vers Plex quand de nouveaux items deviennent disponibles
        try:
            plex_keys  = [
                it["plex_key"] for it in items
                if it.get("plex_key") and it.get("available") and it.get("checked", True)
            ]
            plex_items = []
            for k in plex_keys:
                obj = plex.fetchItem(int(k))
                plex_items.extend(obj.episodes() if obj.type == "season" else [obj])
            if plex_items:
                create_or_update_playlist(plex, playlist["name"], plex_items)
                auto_pushed = True
                current_app.logger.info(
                    "[PLAYLIST] Auto-push '%s' : %d nouveaux, %d total",
                    playlist["name"], newly_available, len(plex_items),
                )
        except Exception:
            current_app.logger.exception("[PLAYLIST] Erreur auto-push load")

    return jsonify({
        "ok":              True,
        "id":              pid,
        "name":            playlist["name"],
        "trakt_url":       playlist.get("trakt_url", ""),
        "items":           items,
        "newly_available": newly_available,
        "auto_pushed":     auto_pushed,
    })


@playlist_bp.route("/plex-playlists")
def list_plex_playlists():
    """Liste toutes les playlists existantes sur le serveur Plex local."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401
    plex_token = session.get("plex_token")
    try:
        plex = _get_local_server(plex_token)
        playlists = plex.playlists()
        return jsonify({
            "ok": True,
            "playlists": [
                {
                    "key":   str(p.ratingKey),
                    "name":  p.title,
                    "count": getattr(p, "leafCount", None),
                }
                for p in playlists
            ],
        })
    except Exception as exc:
        current_app.logger.exception("[PLAYLIST] Erreur list plex playlists")
        return jsonify({"ok": False, "error": str(exc)}), 500


@playlist_bp.route("/plex-playlists/<int:playlist_key>/items")
def plex_playlist_items_by_key(playlist_key):
    """Charge les items d'une playlist Plex existante."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401
    plex_token = session.get("plex_token")
    try:
        plex     = _get_local_server(plex_token)
        playlist = plex.fetchItem(playlist_key)
        result   = []
        for item in playlist.items():
            if item.type == "episode":
                result.append({
                    "key":         str(item.ratingKey),
                    "title":       item.title,
                    "year":        None,
                    "type":        "episode",
                    "plex_title":  getattr(item, "grandparentTitle", ""),
                    "season":      getattr(item, "parentIndex", getattr(item, "seasonNumber", None)),
                    "episode_num": item.index,
                    "show_title":  getattr(item, "grandparentTitle", ""),
                })
            else:
                result.append({
                    "key":         str(item.ratingKey),
                    "title":       item.title,
                    "year":        getattr(item, "year", None),
                    "type":        item.type,
                    "plex_title":  item.title,
                    "season":      None,
                    "episode_num": None,
                    "show_title":  None,
                })
        return jsonify({"ok": True, "name": playlist.title, "items": result})
    except Exception as exc:
        current_app.logger.exception("[PLAYLIST] Erreur load plex playlist items")
        return jsonify({"ok": False, "error": str(exc)}), 500


@playlist_bp.route("/tmdb-search")
def tmdb_search():
    """Recherche TMDB multi (films + séries) pour ajouter des réservés."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401

    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"ok": True, "results": []})

    api_key = load_config().get("TMDB_API_KEY", "").strip()
    if not api_key:
        return jsonify({"ok": False, "error": "TMDB API key not configured"}), 400

    import requests as _req
    try:
        resp = _req.get(
            "https://api.themoviedb.org/3/search/multi",
            params={"api_key": api_key, "query": q, "language": "fr-FR", "include_adult": "false"},
            timeout=10,
        )
        resp.raise_for_status()
        raw = resp.json()
        results = []
        for item in raw.get("results", [])[:12]:
            media_type = item.get("media_type")
            if media_type not in ("movie", "tv"):
                continue
            title = item.get("title") or item.get("name") or ""
            date  = item.get("release_date") or item.get("first_air_date") or ""
            year  = int(date[:4]) if date and len(date) >= 4 and date[:4].isdigit() else None
            results.append({
                "tmdb_id": item["id"],
                "title":   title,
                "year":    year,
                "type":    "movie" if media_type == "movie" else "show",
                "poster":  f"https://image.tmdb.org/t/p/w92{item['poster_path']}" if item.get("poster_path") else None,
            })
        return jsonify({"ok": True, "results": results})
    except Exception as exc:
        current_app.logger.exception("[PLAYLIST] Erreur TMDB search")
        return jsonify({"ok": False, "error": str(exc)}), 500


@playlist_bp.route("/sync-now", methods=["POST"])
def sync_now():
    """Déclenche manuellement la sync des placeholders (même logique que le scheduler)."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401
    plex_token = session.get("plex_token")
    if not plex_token:
        return jsonify({"ok": False, "error": "Token Plex manquant"}), 401
    try:
        from ..services.playlist_scheduler import _run_sync
        _run_sync(plex_token)
        return jsonify({"ok": True})
    except Exception as exc:
        current_app.logger.exception("[PLAYLIST] Erreur sync-now")
        return jsonify({"ok": False, "error": str(exc)}), 500


@playlist_bp.route("/saved/<pid>", methods=["DELETE"])
def delete_saved_playlist(pid):
    """Supprime une playlist sauvegardée."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401
    try:
        delete_playlist(pid)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
    return jsonify({"ok": True})
