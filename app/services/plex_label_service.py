import logging
import re
import threading
import time
from pathlib import Path

import requests
from plexapi.myplex import MyPlexAccount

logger = logging.getLogger(__name__)

STREAMING_PROVIDERS_CA = {
    8:   "Netflix",
    119: "Amazon Prime Video",
    337: "Disney Plus",
    350: "Apple TV Plus",
    230: "Crave",
    531: "Paramount Plus",
    283: "Crunchyroll",
    314: "CBC Gem",
    146: "iciTouTV",
    469: "Club Illico",
    73:  "Tubi TV",
    520: "Discovery Plus",
    11:  "MUBI",
}

# Mapping tag scène → nom de label Plex (doit correspondre aux valeurs de STREAMING_PROVIDERS_CA)
_SCENE_TAG_TO_LABEL = {
    "NF":    "Netflix",
    "AMZN":  "Amazon Prime Video",
    "DSNP":  "Disney Plus",
    "ATVP":  "Apple TV Plus",
    "CRAV":  "Crave",
    "PMTP":  "Paramount Plus",
    "CR":    "Crunchyroll",
    "GEM":   "CBC Gem",
    "TOUTV": "iciTouTV",
    "CLUB":  "Club Illico",
    "TUBI":  "Tubi TV",
    "DSCP":  "Discovery Plus",
    "MUBI":  "MUBI",
}

# Regex qui détecte un tag scène entouré de délimiteurs (point, espace, tiret, début/fin)
_SCENE_TAG_RE = re.compile(
    r'(?:^|[\s.\-])(' + '|'.join(re.escape(t) for t in _SCENE_TAG_TO_LABEL) + r')(?:[\s.\-]|$)',
    re.IGNORECASE,
)

_job_lock = threading.Lock()
_job_status = {
    "running": False,
    "done": False,
    "total": 0,
    "processed": 0,
    "labeled": 0,
    "skipped": 0,
    "errors": 0,
    "log": [],
}


def get_job_status():
    with _job_lock:
        s = dict(_job_status)
        s["log"] = list(_job_status["log"])
        return s


def _update(**kwargs):
    with _job_lock:
        _job_status.update(kwargs)


def _log(msg):
    with _job_lock:
        _job_status["log"].append(msg)
        if len(_job_status["log"]) > 500:
            _job_status["log"] = _job_status["log"][-500:]


def _get_tmdb_id(item):
    for guid in getattr(item, "guids", []):
        gid = str(guid.id)
        if gid.startswith("tmdb://"):
            return gid[7:].split("?")[0]
        if "themoviedb" in gid.lower():
            parts = gid.split("://")
            if len(parts) > 1:
                return parts[1].split("?")[0]
    return None


def _get_label_from_filename(item, providers):
    """
    Cherche un tag scène (NF, DSNP, AMZN…) dans le nom des fichiers média de l'item.
    Retourne le label Plex correspondant si le tag est dans les providers sélectionnés,
    sinon None.
    """
    try:
        for media in getattr(item, "media", []):
            for part in getattr(media, "parts", []):
                fname = Path(part.file).stem if part.file else ""
                m = _SCENE_TAG_RE.search(fname)
                if m:
                    label = _SCENE_TAG_TO_LABEL.get(m.group(1).upper())
                    if label and label in providers.values():
                        return label
    except Exception:
        pass
    return None


def _get_streaming_labels(tmdb_id, media_type, api_key, watch_region, providers):
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/watch/providers"
    try:
        resp = requests.get(url, params={"api_key": api_key}, timeout=10)
        if resp.status_code != 200:
            return set()
        region_data = resp.json().get("results", {}).get(watch_region, {})
        # TMDb trie les providers par display_priority (le plus pertinent en premier).
        # On prend uniquement le premier match dans chaque catégorie par priorité.
        for category in ("flatrate", "free", "ads"):
            for p in region_data.get(category, []):
                pid = p.get("provider_id")
                if pid in providers:
                    return {providers[pid]}
        return set()
    except Exception as e:
        logger.warning(f"[LabelService] TMDb error for {media_type}/{tmdb_id}: {e}")
        return set()


def _worker(plex_token, api_key, library_configs, watch_region, providers):
    _update(running=True, done=False, total=0, processed=0,
            labeled=0, skipped=0, errors=0, log=[])
    try:
        account = MyPlexAccount(token=plex_token)
        server = next(
            (s for s in account.resources()
             if s.provides == "server" and getattr(s, "owned", False)),
            None,
        )
        if not server:
            _log("❌ Serveur Plex local introuvable")
            return

        plex = server.connect()

        all_items = []
        for lib_name, media_type in library_configs:
            try:
                section = plex.library.section(lib_name)
                for item in section.all():
                    all_items.append((item, media_type))
            except Exception as e:
                _log(f"❌ Bibliothèque '{lib_name}': {e}")

        _update(total=len(all_items))
        _log(f"▶ {len(all_items)} item(s) dans {len(library_configs)} bibliothèque(s)")

        labeled = skipped = errors = 0

        for i, (item, media_type) in enumerate(all_items):
            try:
                existing = {lbl.tag for lbl in item.labels}

                # Étape 1 : tag scène dans le nom de fichier (priorité)
                label = _get_label_from_filename(item, providers)
                source = "fichier"

                # Étape 2 : fallback TMDb si aucun tag trouvé dans le fichier
                if not label:
                    tmdb_id = _get_tmdb_id(item)
                    if not tmdb_id:
                        skipped += 1
                        _update(processed=i + 1, skipped=skipped)
                        time.sleep(0.05)
                        continue
                    services = _get_streaming_labels(tmdb_id, media_type, api_key, watch_region, providers)
                    label = next(iter(services), None)
                    source = "TMDb"

                if label and label not in existing:
                    item.addLabel(label)
                    labeled += 1
                    _log(f"✅ {item.title} → {label} ({source})")

                _update(processed=i + 1, labeled=labeled, skipped=skipped, errors=errors)

            except Exception as e:
                errors += 1
                _log(f"❌ {item.title}: {e}")
                _update(processed=i + 1, errors=errors)

            time.sleep(0.25)

        _log(f"✔ Terminé — {labeled} labellisés, {skipped} sans ID TMDb, {errors} erreur(s)")

    except Exception as e:
        _log(f"❌ Erreur fatale: {e}")
        logger.error("[LabelService] Fatal error", exc_info=True)
    finally:
        _update(running=False, done=True)


def start_label_job(plex_token, api_key, library_configs, watch_region, providers):
    """
    library_configs : list of (lib_name, media_type) tuples
    providers       : dict {provider_id: label_name}
    """
    t = threading.Thread(
        target=_worker,
        args=(plex_token, api_key, library_configs, watch_region, providers),
        daemon=True,
    )
    t.start()
