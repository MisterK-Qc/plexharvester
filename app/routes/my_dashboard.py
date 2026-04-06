import threading

from flask import Blueprint, render_template, session, redirect, url_for, current_app, jsonify, request
from plexapi.myplex import MyPlexAccount
from collections import Counter

from ..services.plex_service import connect_to_server, get_video_bitrate
from ..services.tmdb_service import find_streaming_offers_tmdb, normalize_service_name
from ..services.stats_service import normalize_genre, parse_resolution, build_empty_stats, build_stats_context
from ..services.cache_service import load_cache, save_cache, is_cache_valid

my_dashboard_bp = Blueprint("my_dashboard", __name__)

_analysis_lock = threading.Lock()
_analysis_running = False
_analysis_cancel = threading.Event()


@my_dashboard_bp.route("/my_dashboard")
def my_dashboard():
    current_app.logger.debug("=== MY_DASHBOARD HIT AVANT CACHE ===")

    if not session.get("logged_in"):
        return redirect(url_for("auth.login"))

    plex_token = session.get("plex_token")
    if not plex_token:
        return redirect(url_for("auth.login"))

    local_selected_libraries = session.get("local_selected_libraries", [])

    api_key = current_app.config.get("TMDB_API_KEY", "")
    tmdb_configured = bool(api_key)

    force_refresh = request.args.get("refresh", "false").lower() == "true"
    refresh_delay = int(current_app.config.get("REFRESH_DELAY_DAYS", 30))

    # Vérification rapide du flag avant de connecter
    global _analysis_running
    _lock_acquired = False
    if not force_refresh and _analysis_running:
        current_app.logger.info("=== MY_DASHBOARD analyse déjà en cours (pré-connexion), requête ignorée ===")
        return render_template("my_dashboard_loading.html"), 202

    try:
        account = MyPlexAccount(token=plex_token)
        resources = account.resources()
        my_server_res = next(
            (s for s in resources if s.provides == "server" and getattr(s, "owned", False)),
            None
        )
        if not my_server_res:
            return render_template("error.html",
                code=500,
                icon="🖥️",
                message="Aucun serveur Plex possédé trouvé sur ce compte.",
                detail=None
            ), 500

        target_server = connect_to_server(my_server_res, plex_token, prefer_local=True)
        if not target_server:
            return render_template("error.html",
                code=500,
                icon="🔌",
                message="Impossible de se connecter au serveur Plex local.",
                detail=None
            ), 500

        current_app.logger.debug(f"=== MY_DASHBOARD CONNECTED SERVER: {target_server.friendlyName} ===")

        all_sections = target_server.library.sections()

        available_titles = [s.title for s in all_sections]

        if not local_selected_libraries:
            local_selected_libraries = available_titles
        else:
            local_selected_libraries = [
                title for title in local_selected_libraries
                if title in available_titles
            ]

        if not local_selected_libraries:
            local_selected_libraries = available_titles

        if not local_selected_libraries:
            local_selected_libraries = [s.title for s in all_sections]

        # Check cache ici : on connaît maintenant le nom du serveur et les libs effectives
        cache_libs_key = "_".join(sorted(local_selected_libraries))
        cache_key = f"my_dashboard_{target_server.friendlyName}_{cache_libs_key}"
        cache = load_cache(cache_key)
        if not force_refresh and is_cache_valid(cache, refresh_delay):
            current_app.logger.debug(f"=== MY_DASHBOARD CACHE HIT: {cache_key} ===")
            data = cache["data"]
            return render_template(
                "my_dashboard.html",
                tmdb_configured=tmdb_configured,
                results_stream=data["results_stream"],
                results_purchase=data["results_purchase"],
                watchlist_items=data["watchlist_items"],
                stats=data["stats"]
            )

        # Pas de cache valide → acquérir le lock avant l'analyse longue
        with _analysis_lock:
            if _analysis_running:
                current_app.logger.info("=== MY_DASHBOARD analyse déjà en cours, requête ignorée ===")
                return render_template("my_dashboard_loading.html"), 202
            _analysis_running = True
            _analysis_cancel.clear()
            _lock_acquired = True

        results_stream_by_lib = {}
        results_purchase_by_lib = {}
        watchlist_items = []

        total_items = 0
        total_movies = 0
        total_shows = 0
        total_albums = 0
        all_artist_guids = set()

        resolution_counter = Counter()
        service_counter = Counter()
        genre_counter = Counter()
        movie_genre_counter = Counter()
        show_genre_counter = Counter()

        def get_media_resolution_info(media):
            try:
                if not media or not getattr(media, "media", None):
                    return None, None, None
                media_obj = media.media[0]
                resolution = parse_resolution(media_obj.videoResolution)
                codec = media_obj.videoCodec
                bitrate = get_video_bitrate(media_obj)
                return resolution, codec, bitrate
            except Exception:
                return None, None, None

        def get_show_first_episode_info(show):
            try:
                for season in show.seasons():
                    episodes = season.episodes()
                    if episodes:
                        return get_media_resolution_info(episodes[0])
            except Exception:
                pass
            return None, None, None

        def process_media_items(section, limit=0):
            nonlocal total_items, total_movies, total_shows

            stream_results = []
            purchase_results = []

            for i, media in enumerate(section.all()):
                if limit > 0 and i >= limit:
                    break

                try:
                    title = (
                        getattr(media, "originalTitle", None)
                        or getattr(media, "title", None)
                        or "Unknown"
                    ).strip()

                    year = getattr(media, "year", 0)
                    media_type_label = "Film" if section.type == "movie" else "Série"

                    media_labels = {
                        normalize_service_name(lbl.tag)
                        for lbl in getattr(media, "labels", [])
                    }

                    resolution, codec, bitrate = None, None, None

                    if section.type == "movie":
                        resolution, codec, bitrate = get_media_resolution_info(media)
                    elif section.type == "show":
                        resolution, codec, bitrate = get_show_first_episode_info(media)

                    stream_avail, purchase_avail = {}, {}
                    tmdb_id, tmdb_total_episodes = None, None

                    if tmdb_configured:
                        stream_avail, purchase_avail, tmdb_id, tmdb_total_episodes = find_streaming_offers_tmdb(
                            api_key,
                            title,
                            year,
                            "tv" if section.type == "show" else "movie"
                        )

                    for label in media_labels:
                        service_counter[label] += 1

                    plex_episode_count = None
                    episodes_missing = False
                    missing_count = None

                    if section.type == "show":
                        try:
                            plex_episode_count = getattr(media, "leafCount", None)
                            if plex_episode_count is None:
                                plex_episode_count = len(media.episodes())
                        except Exception:
                            plex_episode_count = None

                        if (
                            isinstance(plex_episode_count, int)
                            and isinstance(tmdb_total_episodes, int)
                            and tmdb_total_episodes > 0
                            and plex_episode_count < tmdb_total_episodes
                        ):
                            episodes_missing = True
                            missing_count = tmdb_total_episodes - plex_episode_count

                    total_items += 1

                    if media_type_label == "Film":
                        total_movies += 1
                        for genre in getattr(media, "genres", []) or []:
                            movie_genre_counter[normalize_genre(genre.tag)] += 1

                    elif media_type_label == "Série":
                        total_shows += 1
                        for genre in getattr(media, "genres", []) or []:
                            show_genre_counter[normalize_genre(genre.tag)] += 1

                    if resolution:
                        if resolution >= 2160:
                            resolution_counter["4K"] += 1
                        elif resolution >= 1080:
                            resolution_counter["1080p"] += 1
                        elif resolution >= 720:
                            resolution_counter["720p"] += 1
                        elif resolution >= 480:
                            resolution_counter["480p"] += 1
                        else:
                            resolution_counter["Autre"] += 1
                    else:
                        resolution_counter["Inconnue"] += 1

                    item_data = {
                        "title": title,
                        "type": media_type_label,
                        "year": year,
                        "codec": codec,
                        "bitrate": bitrate,
                        "resolution": resolution,
                        "plex_episodes": plex_episode_count if section.type == "show" else None,
                        "tmdb_total_episodes": tmdb_total_episodes if section.type == "show" else None,
                        "tmdb_id": tmdb_id if section.type == "show" else None,
                        "episodes_missing": episodes_missing if section.type == "show" else False,
                        "episodes_missing_count": missing_count if section.type == "show" else None,
                    }

                    if stream_avail:
                        stream_item_data = item_data.copy()
                        stream_item_data["services"] = stream_avail
                        stream_results.append(stream_item_data)

                    if purchase_avail:
                        purchase_item_data = item_data.copy()
                        purchase_item_data["purchase"] = purchase_avail
                        purchase_results.append(purchase_item_data)

                except Exception:
                    continue

            return stream_results, purchase_results

        watchlist_section = next(
            (
                s for s in all_sections
                if s.title.lower() in {"watchlist", "ma liste", "liste de suivi"}
            ),
            None
        )

        processed_lib_count = 0

        for section in all_sections:
            if _analysis_cancel.is_set():
                current_app.logger.info("[MY_DASHBOARD] Analyse annulée par l'utilisateur.")
                return render_template("error.html",
                    code=200,
                    icon="⛔",
                    message="Analyse annulée.",
                    detail="Vous pouvez relancer depuis Mon Serveur."
                )

            if section.title not in local_selected_libraries:
                continue

            if section.type in ["movie", "show"]:
                processed_lib_count += 1
                stream_results, purchase_results = process_media_items(section)

                final_stream_services_logos = {}
                for item in stream_results:
                    for service_name, service_data in item.get("services", {}).items():
                        if service_name not in final_stream_services_logos and service_data.get("logo"):
                            final_stream_services_logos[service_name] = service_data["logo"]
                stream_service_names = sorted(final_stream_services_logos.keys())

                results_stream_by_lib[section.title] = {
                    "items": stream_results,
                    "services": stream_service_names,
                    "logos": final_stream_services_logos
                }

                final_purchase_services_logos = {}
                for item in purchase_results:
                    for service_name, service_data in item.get("purchase", {}).items():
                        if service_name not in final_purchase_services_logos and service_data.get("logo"):
                            final_purchase_services_logos[service_name] = service_data["logo"]
                purchase_service_names = sorted(final_purchase_services_logos.keys())

                results_purchase_by_lib[section.title] = {
                    "items": purchase_results,
                    "services": purchase_service_names,
                    "logos": final_purchase_services_logos
                }

            elif section.type in ["music", "artist"]:
                try:
                    all_albums_in_section = section.albums()
                    if not all_albums_in_section:
                        continue

                    total_albums += len(all_albums_in_section)
                    for album in all_albums_in_section:
                        if hasattr(album, "parentGuid"):
                            all_artist_guids.add(album.parentGuid)
                        for genre in getattr(album, "genres", []) or []:
                            genre_counter[normalize_genre(genre.tag)] += 1
                except Exception:
                    pass

        if watchlist_section and tmdb_configured:
            stream_results, _ = process_media_items(watchlist_section, limit=100)
            watchlist_items = stream_results

        stats = build_stats_context(
            total_items=total_items,
            total_movies=total_movies,
            total_shows=total_shows,
            total_artists=len(all_artist_guids),
            total_albums=total_albums,
            libs_count=processed_lib_count,
            service_counter=service_counter,
            resolution_counter=resolution_counter,
            genre_counter=genre_counter,
            movie_genre_counter=movie_genre_counter,
            show_genre_counter=show_genre_counter
        )

        save_cache(cache_key, {
            "results_stream": results_stream_by_lib,
            "results_purchase": results_purchase_by_lib,
            "watchlist_items": watchlist_items,
            "stats": stats
        })

        return render_template(
            "my_dashboard.html",
            tmdb_configured=tmdb_configured,
            results_stream=results_stream_by_lib,
            results_purchase=results_purchase_by_lib,
            watchlist_items=watchlist_items,
            stats=stats
        )

    except Exception as e:
        return render_template("error.html",
            code=500,
            icon="💥",
            message="Une erreur s'est produite lors du chargement de Mon Serveur.",
            detail=str(e)
        ), 500

    finally:
        if _lock_acquired:
            with _analysis_lock:
                _analysis_running = False


@my_dashboard_bp.route("/my_dashboard_status")
def my_dashboard_status():
    if not session.get("logged_in"):
        return jsonify({"running": False}), 200
    return jsonify({"running": _analysis_running}), 200


@my_dashboard_bp.route("/my_dashboard_cancel", methods=["POST"])
def my_dashboard_cancel():
    if not session.get("logged_in"):
        return jsonify({"success": False}), 401
    if _analysis_running:
        _analysis_cancel.set()
        return jsonify({"success": True, "message": "Annulation demandée."})
    return jsonify({"success": False, "message": "Aucune analyse en cours."}), 400


@my_dashboard_bp.route("/search_tmdb", methods=["POST"])
def search_tmdb():
    if not session.get("logged_in"):
        return jsonify({"error": "Not logged in"}), 401

    data = request.json or {}
    title = data.get("title")
    media_type = data.get("type", "movie")

    if not title:
        return jsonify({"error": "Title is required"}), 400

    api_key = current_app.config.get("TMDB_API_KEY", "")
    stream_avail, purchase_avail, _, _ = find_streaming_offers_tmdb(api_key, title, media_type=media_type)

    return jsonify({
        "streaming": stream_avail,
        "purchase": purchase_avail
    })