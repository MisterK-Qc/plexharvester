import json
import logging
import os

LANG_DIR = os.path.join(os.path.dirname(__file__), "languages")

_cache = {}  # { "fr": {key: value}, "en": {...} }

def load_language(lang_code: str = "fr") -> dict:
    """Charge (et met en cache) le dictionnaire de langue demandé."""
    lang_code = (lang_code or "fr").lower()
    if lang_code in _cache:
        return _cache[lang_code]

    path = os.path.join(LANG_DIR, f"{lang_code}.json")
    if not os.path.exists(path):
        logging.warning(f"[i18n] Fichier de langue introuvable: {path}, fallback fr.json")
        path = os.path.join(LANG_DIR, "fr.json")

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        _cache[lang_code] = data
    except Exception as e:
        logging.error(f"[i18n] Erreur de lecture de {path}: {e}", exc_info=True)
        data = {}
        _cache[lang_code] = data

    return data

def translate(lang_code: str, key: str, **kwargs) -> str:
    """Retourne la traduction pour une clé donnée, avec interpolation éventuelle."""
    lang = load_language(lang_code)
    text = lang.get(key, key)  # fallback = la clé brute
    try:
        if kwargs:
            text = text.format(**kwargs)
    except Exception as e:
        logging.error(f"[i18n] Erreur de formatage sur la clé {key}: {e}", exc_info=True)
    return text
