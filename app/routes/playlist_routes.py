from flask import Blueprint, render_template, request, session, redirect, url_for, jsonify, current_app

from ..services.config_service import load_config
from ..services.trakt_service import fetch_trakt_list
from ..services.playlist_service import import_trakt_to_plex

playlist_bp = Blueprint("playlist", __name__, url_prefix="/playlists")


@playlist_bp.route("/")
def playlists_page():
    if not session.get("logged_in"):
        return redirect(url_for("auth.login"))
    cfg = load_config()
    return render_template(
        "playlists.html",
        trakt_client_id=cfg.get("TRAKT_CLIENT_ID", ""),
    )


@playlist_bp.route("/import", methods=["POST"])
def import_playlist():
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401

    plex_token = session.get("plex_token")
    if not plex_token:
        return jsonify({"ok": False, "error": "Token Plex manquant"}), 401

    data         = request.get_json(silent=True) or {}
    trakt_url    = (data.get("trakt_url")     or "").strip()
    playlist_name = (data.get("playlist_name") or "").strip()
    client_id    = (data.get("client_id")     or "").strip()

    # Fallback sur le Client ID sauvegardé en config
    if not client_id:
        cfg = load_config()
        client_id = cfg.get("TRAKT_CLIENT_ID", "").strip()

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

    if not playlist_name:
        playlist_name = list_name

    if not trakt_items:
        return jsonify({
            "ok": True,
            "list_name": list_name,
            "playlist_name": playlist_name,
            "total": 0,
            "matched": 0,
            "unmatched": 0,
            "matched_items": [],
            "unmatched_items": [],
        })

    try:
        report = import_trakt_to_plex(plex_token, playlist_name, trakt_items)
    except Exception as exc:
        current_app.logger.exception("[PLAYLIST] Erreur import Plex")
        return jsonify({"ok": False, "error": f"Erreur Plex : {exc}"}), 500

    return jsonify({
        "ok":           True,
        "list_name":    list_name,
        "playlist_name": playlist_name,
        "total":        len(trakt_items),
        "matched":      report["matched_count"],
        "unmatched":    report["unmatched_count"],
        "matched_items": [
            {
                "title":      m["trakt"]["title"],
                "year":       m["trakt"]["year"],
                "plex_title": m["plex"].title,
                "type":       m["trakt"]["type"],
            }
            for m in report["matched"]
        ],
        "unmatched_items": [
            {
                "title":   u["title"],
                "year":    u["year"],
                "type":    u["type"],
                "tmdb_id": u.get("tmdb_id"),
            }
            for u in report["unmatched"]
        ],
    })
