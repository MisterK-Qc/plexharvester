import os
import shutil
import logging


def get_disk_usage_info(path: str) -> dict:
    """
    Retourne les infos d'espace disque pour le chemin donné.
    """
    try:
        if not path or not os.path.exists(path):
            return {
                "path": path,
                "exists": False,
                "total": 0,
                "used": 0,
                "free": 0,
                "free_human": "N/A",
                "used_human": "N/A",
                "total_human": "N/A",
                "percent_used": 0,
            }

        usage = shutil.disk_usage(path)
        percent_used = round((usage.used / usage.total) * 100, 1) if usage.total > 0 else 0

        return {
            "path": path,
            "exists": True,
            "total": usage.total,
            "used": usage.used,
            "free": usage.free,
            "free_human": human_size(usage.free),
            "used_human": human_size(usage.used),
            "total_human": human_size(usage.total),
            "percent_used": percent_used,
        }
    except Exception as e:
        logging.debug(f"[STORAGE] erreur sur path {path}: {e}")
        return {
            "path": path,
            "exists": False,
            "total": 0,
            "used": 0,
            "free": 0,
            "free_human": "N/A",
            "used_human": "N/A",
            "total_human": "N/A",
            "percent_used": 0,
        }


def human_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(num_bytes)

    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024.0