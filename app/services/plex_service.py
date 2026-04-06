import logging
from plexapi.server import PlexServer


def connect_to_server(resource, token, prefer_local=True):
    if not resource:
        return None
    try:
        if prefer_local:
            local_conn = next((c for c in resource.connections if getattr(c, "local", False)), None)
            if local_conn:
                logging.info(f"Connexion locale à {resource.name} via {local_conn.uri}")
                return PlexServer(local_conn.uri, token, timeout=30)
        logging.info(f"Connexion standard à {resource.name}")
        return resource.connect(timeout=30)
    except Exception as e:
        logging.error(f"Impossible de se connecter à {resource.name}: {e}")
        return None


def normalize_name(name):
    if not name:
        return ''
    return ''.join(ch.lower() for ch in name if ch.isalnum())


def get_video_bitrate(media):
    if not media:
        return 0
    best = 0
    try:
        if getattr(media, "parts", None):
            for part in media.parts:
                for stream in getattr(part, "streams", []):
                    if getattr(stream, "streamType", None) == 1 and getattr(stream, "bitrate", None):
                        try:
                            best = max(best, int(stream.bitrate))
                        except Exception:
                            pass
        if best == 0 and getattr(media, "bitrate", None):
            best = int(media.bitrate)
    except Exception:
        pass
    return best


def serialize_section(section):
    import logging
    logger = logging.getLogger(__name__)

    all_items = list(section.all())
    total = len(all_items)
    logger.info("[PLEX] serialize_section '%s' — %s items à charger", section.title, total)

    # Pour les sections de séries : un seul appel bulk pour tous les épisodes
    # au lieu de N appels individuels item.episodes() — drastiquement plus rapide
    # sur un serveur distant (réduit de N requêtes réseau à 2).
    episodes_by_show_key = {}
    if all_items and all_items[0].type == 'show':
        try:
            logger.debug("[PLEX] Bulk fetch épisodes pour '%s'...", section.title)
            all_eps = section.all(libtype='episode')
            for ep in all_eps:
                key = getattr(ep, 'grandparentRatingKey', None)
                if key is not None:
                    episodes_by_show_key.setdefault(key, []).append(ep)
            logger.info("[PLEX] %d épisodes chargés en bulk pour '%s'", len(all_eps), section.title)
        except Exception as exc:
            logger.warning("[PLEX] Bulk episode fetch échoué pour '%s': %s — fallback per-show", section.title, exc)

    items = []
    for idx, item in enumerate(all_items, start=1):
        item_dict = {
            'title': getattr(item, 'title', None),
            'originalTitle': getattr(item, 'originalTitle', None),
            'year': getattr(item, 'year', None),
            'guid': getattr(item, 'guid', None),
            'type': item.type,
            'labels': [lbl.tag for lbl in getattr(item, 'labels', [])],
        }

        if item.type == 'movie':
            media = item.media[0] if getattr(item, "media", None) else None
            item_dict.update({
                'res': media.videoResolution if media else '',
                'codec': (media.videoCodec or '').upper() if media else '',
                'bitrate': get_video_bitrate(media) if media else 0
            })

        elif item.type == 'show':
            media = None
            episodes_data = []
            if idx % 10 == 0 or idx == total:
                logger.debug(
                    "[PLEX] serialize_section '%s' — séries %s/%s (dernière: %s)",
                    section.title, idx, total, item.title
                )
            try:
                # Utilise les épisodes pre-fetchés en bulk si disponibles
                eps = episodes_by_show_key.get(item.ratingKey)
                if eps is None:
                    eps = item.episodes()  # fallback per-show

                if eps:
                    first_ep = eps[0]
                    media = first_ep.media[0] if getattr(first_ep, "media", None) else None

                for ep in eps:
                    ep_media = ep.media[0] if getattr(ep, "media", None) else None
                    episodes_data.append({
                        'season': ep.seasonNumber,
                        'episode': ep.index,
                        'title': ep.title,
                        'res': ep_media.videoResolution if ep_media else '',
                        'codec': (ep_media.videoCodec or '').upper() if ep_media else '',
                        'bitrate': get_video_bitrate(ep_media) if ep_media else 0
                    })
            except Exception:
                episodes_data = []

            item_dict.update({
                'res': media.videoResolution if media else '',
                'codec': (media.videoCodec or '').upper() if media else '',
                'bitrate': get_video_bitrate(media) if media else 0,
                'episode_count': len(episodes_data),
                'episodes': episodes_data
            })

        elif item.type == 'artist':
            albums_list = []
            try:
                for album in item.albums():
                    tracks = album.tracks() or []
                    first_track = tracks[0] if tracks else None
                    media = first_track.media[0] if first_track and first_track.media else None
                    albums_list.append({
                        'title': album.title,
                        'year': album.year,
                        'codec': (media.audioCodec.upper() if media and media.audioCodec else ''),
                        'bitrate': (media.bitrate if media and media.bitrate else 0)
                    })
            except Exception:
                pass

            item_dict['albums'] = albums_list

        items.append(item_dict)

    return items


def build_indexes(items):
    by_guid = {
        (item.get('guid') or '').strip().lower(): item
        for item in items if item.get('guid')
    }
    by_title_year = {
        ((item.get('title') or '').strip().lower(), str(item.get('year', ''))): item
        for item in items if item.get('title')
    }
    by_orig_title_year = {
        ((item.get('originalTitle') or '').strip().lower(), str(item.get('year', ''))): item
        for item in items if item.get('originalTitle')
    }
    by_norm_title_year = {
        (normalize_name(item.get('title') or ''), str(item.get('year', ''))): item
        for item in items if item.get('title')
    }
    by_norm_orig_title_year = {
        (normalize_name(item.get('originalTitle') or ''), str(item.get('year', ''))): item
        for item in items if item.get('originalTitle')
    }

    return {
        "by_guid": by_guid,
        "by_title_year": by_title_year,
        "by_orig_title_year": by_orig_title_year,
        "by_norm_title_year": by_norm_title_year,
        "by_norm_orig_title_year": by_norm_orig_title_year,
    }


def find_match(remote_item, indexes):
    item_guid = (remote_item.get('guid') or '').strip().lower()
    item_year = str(remote_item.get('year', ''))
    remote_title = (remote_item.get('title') or '').strip().lower()
    remote_orig_title = (remote_item.get('originalTitle') or '').strip().lower()

    return (
        indexes["by_guid"].get(item_guid)
        or indexes["by_orig_title_year"].get((remote_orig_title, item_year))
        or indexes["by_orig_title_year"].get((remote_title, item_year))
        or indexes["by_norm_orig_title_year"].get((normalize_name(remote_item.get('title') or ''), item_year))
        or indexes["by_title_year"].get((remote_title, item_year))
        or indexes["by_norm_title_year"].get((normalize_name(remote_item.get('title') or ''), item_year))
    )