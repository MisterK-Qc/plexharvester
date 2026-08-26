import os

CONFIG_DIR = os.environ.get("PLEX_COMPARE_CONFIG_DIR", "/config")

LOG_DIR = os.path.join(CONFIG_DIR, "logs")
CACHE_DIR = os.path.join(CONFIG_DIR, "cache")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")
MKV_LANGUAGES_FILE = os.path.join(CONFIG_DIR, "mkv_languages.json")
PLAYLISTS_STORE_FILE = os.path.join(CONFIG_DIR, "playlists_store.json")
SPOTIFY_ARCHIVE_FILE = os.path.join(CONFIG_DIR, "spotify_archive.spotdl")

# Création automatique des dossiers
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)