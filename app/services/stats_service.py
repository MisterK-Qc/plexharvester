GENRE_NORMALIZATION_MAP = {
    "action": "Action",
    "adventure": "Aventure",
    "animation": "Animation",
    "comedy": "Comédie",
    "crime": "Crime",
    "documentary": "Documentaire",
    "drama": "Drame",
    "family": "Famille",
    "familial": "Famille",
    "fantasy": "Fantastique",
    "history": "Histoire",
    "horror": "Horreur",
    "music": "Musique",
    "mystery": "Mystère",
    "romance": "Romance",
    "science fiction": "Science-Fiction",
    "science-fiction": "Science-Fiction",
    "tv movie": "Téléfilm",
    "thriller": "Thriller",
    "war": "Guerre",
    "western": "Western",
    "action & adventure": "Action & Aventure",
    "sci-fi & fantasy": "Science-Fiction & Fantastique",
}


def normalize_genre(genre_tag):
    if not genre_tag:
        return "Inconnu"
    normalized = genre_tag.lower().strip()
    return GENRE_NORMALIZATION_MAP.get(normalized, genre_tag.capitalize())


def parse_resolution(res_str):
    try:
        if not res_str:
            return None
        res_str = str(res_str).lower()
        if '4k' in res_str:
            return 2160
        elif '1080' in res_str:
            return 1080
        elif '720' in res_str:
            return 720
        elif 'sd' in res_str:
            return 480
        elif res_str.endswith('p'):
            return int(res_str.replace('p', ''))
    except Exception:
        return None
    return None


def build_empty_stats():
    return {
        "total_items": 0,
        "total_movies": 0,
        "total_shows": 0,
        "total_artists": 0,
        "total_albums": 0,
        "average_per_lib": 0,
        "top_services": [],
        "resolutions": {},
        "genres": {},
        "movie_genres": {},
        "show_genres": {}
    }


def build_stats_context(total_items, total_movies, total_shows, total_artists, total_albums,
                        libs_count, service_counter, resolution_counter, genre_counter,
                        movie_genre_counter, show_genre_counter):
    average_per_lib = round(total_items / max(libs_count, 1), 1)
    top_services = service_counter.most_common(10)

    return {
        "total_items": total_items,
        "total_movies": total_movies,
        "total_shows": total_shows,
        "total_artists": total_artists,
        "total_albums": total_albums,
        "average_per_lib": average_per_lib,
        "top_services": top_services,
        "resolutions": dict(resolution_counter),
        "genres": dict(genre_counter),
        "movie_genres": dict(movie_genre_counter),
        "show_genres": dict(show_genre_counter)
    }