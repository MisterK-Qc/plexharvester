import json
import os
import re
from datetime import datetime, timedelta, timezone

from app.config_paths import CACHE_DIR


def _safe_key(key: str) -> str:
    return re.sub(r'[^a-zA-Z0-9._-]+', '_', str(key))


def _cache_path(key: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{_safe_key(key)}.json")


def load_cache(key: str):
    path = _cache_path(key)
    if not os.path.exists(path):
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_cache(key: str, data: dict):
    path = _cache_path(key)
    tmp_path = path + ".tmp"
    payload = {
        "last_update": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, path)  # atomique sur Linux et Windows NTFS
    except Exception:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        raise


def delete_cache(key: str) -> bool:
    """Supprime le fichier de cache. Retourne True si supprimé."""
    path = _cache_path(key)
    try:
        if os.path.exists(path):
            os.remove(path)
            return True
    except Exception:
        pass
    return False


def delete_caches_by_prefix(prefix: str) -> int:
    """Supprime tous les fichiers de cache dont la clé commence par prefix. Retourne le nombre supprimé."""
    safe_prefix = re.sub(r'[^a-zA-Z0-9._-]+', '_', str(prefix))
    count = 0
    try:
        for fname in os.listdir(CACHE_DIR):
            if fname.startswith(safe_prefix) and fname.endswith(".json"):
                try:
                    os.remove(os.path.join(CACHE_DIR, fname))
                    count += 1
                except Exception:
                    pass
    except Exception:
        pass
    return count


def is_cache_valid(cache: dict | None, delay_days: int) -> bool:
    if not cache:
        return False

    try:
        last_update = datetime.fromisoformat(cache["last_update"])
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - last_update < timedelta(days=delay_days)
    except Exception:
        return False