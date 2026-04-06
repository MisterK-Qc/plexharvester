import logging
import re
import unicodedata

from app.services.ftp_index_service import (
    find_ftp_matches_for_movie,
    find_ftp_match_for_episode,
)

# =========================================================
# Normalisation / matching
# =========================================================

COMPARE_STOPWORDS = {
    "the", "a", "an", "of", "and", "et", "la", "le", "les",
    "des", "du", "de", "d", "l"
}

SERIES_TOKEN_EQUIVALENTS = {
    "zero": "0",
    "zéro": "0",
    "un": "1",
    "une": "1",
    "one": "1",
    "deux": "2",
    "two": "2",
    "trois": "3",
    "three": "3",
    "quatre": "4",
    "four": "4",
    "cinq": "5",
    "five": "5",
    "six": "6",
    "sept": "7",
    "seven": "7",
    "huit": "8",
    "eight": "8",
    "neuf": "9",
    "nine": "9",
    "dix": "10",
    "ten": "10",
}


def _strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", str(text or ""))
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def _roman_to_int_token(tok: str) -> str:
    roman_map = {
        "ii": "2",
        "iii": "3",
        "iv": "4",
        "v": "5",
        "vi": "6",
        "vii": "7",
        "viii": "8",
        "ix": "9",
        "x": "10",
    }
    return roman_map.get(tok, tok)


def normalize_name(text: str) -> str:
    """
    Normalisation générique pour matching simple.
    """
    if not text:
        return ""

    text = _strip_accents(text).lower()
    text = text.replace("&", " and ")
    text = text.replace("'", " ")  # apostrophe → espace pour préserver les tokens
    text = re.sub(r"\[[^\]]*\]", " ", text)
    text = re.sub(r"\([^\)]*\)", " ", text)
    text = re.sub(r"[._\-:/]+", " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    tokens = []
    for tok in text.split():
        tok = _roman_to_int_token(tok)
        if tok in COMPARE_STOPWORDS:
            continue
        tokens.append(tok)

    return " ".join(tokens).strip()


def normalize_series_name(text: str) -> str:
    """
    Normalisation spéciale séries:
    - accents retirés
    - nombres en mots -> chiffres
    """
    norm = normalize_name(text)
    if not norm:
        return ""

    tokens = []
    for tok in norm.split():
        tok = SERIES_TOKEN_EQUIVALENTS.get(tok, tok)
        tok = _roman_to_int_token(tok)
        if tok in COMPARE_STOPWORDS:
            continue
        tokens.append(tok)

    return " ".join(tokens).strip()


def title_tokens(text: str):
    return [tok for tok in normalize_name(text).split() if tok]


def series_tokens(text: str):
    return [tok for tok in normalize_series_name(text).split() if tok]


def token_set_similarity(a_tokens, b_tokens):
    if not a_tokens or not b_tokens:
        return 0.0
    a = set(a_tokens)
    b = set(b_tokens)
    inter = len(a & b)
    denom = max(len(a), len(b))
    return inter / denom if denom else 0.0


def token_containment(a_tokens, b_tokens):
    if not a_tokens:
        return 0.0
    a = set(a_tokens)
    b = set(b_tokens)
    return len(a & b) / len(a)


def _safe_int(value):
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


# =========================================================
# Résolution / qualité
# =========================================================

CODEC_RANK = {
    # AV1
    "av1": 6,
    # HEVC / H.265
    "hevc": 5, "h265": 5, "x265": 5,
    # H.264 / AVC
    "h264": 4, "avc": 4, "x264": 4,
    # MPEG-4 / DivX / Xvid
    "mpeg4": 3, "divx": 3, "xvid": 3, "mp4v": 3,
    # MPEG-2 / VC-1
    "mpeg2": 2, "vc1": 2, "wmv3": 2,
    # MPEG-1 et autres
    "mpeg1": 1,
}


def codec_rank(codec: str) -> int:
    if not codec:
        return 0
    return CODEC_RANK.get(str(codec).lower().strip(), 0)

def resolution_rank(res):
    if res is None:
        return 0

    if isinstance(res, (int, float)):
        v = int(res)
        if v >= 2000:
            return 4
        if v >= 1000:
            return 3
        if v >= 700:
            return 2
        if v > 0:
            return 1
        return 0

    s = str(res).lower()

    if "4k" in s or "2160" in s:
        return 4
    if "1080" in s:
        return 3
    if "720" in s:
        return 2
    if "480" in s or "576" in s or "sd" in s:
        return 1

    try:
        v = int("".join(ch for ch in s if ch.isdigit()))
        if v >= 2000:
            return 4
        if v >= 1000:
            return 3
        if v >= 700:
            return 2
        if v > 0:
            return 1
    except ValueError:
        pass

    return 0


def _local_item_passes_label_filter(local, comparison_settings):
    show_only_labels = [str(lbl).lower() for lbl in (comparison_settings.get("show_only_labels") or [])]
    if not show_only_labels:
        return True

    local_labels = [str(lbl).lower() for lbl in (local.get("labels") or [])]

    # Si aucun label local, on laisse passer comme avant
    if not local_labels:
        return True

    return any(lbl in local_labels for lbl in show_only_labels)


def _compute_quality_flags(remote, local, comparison_settings):
    r_res = remote.get("res")
    l_res = local.get("res")

    r_bitrate = remote.get("bitrate")
    l_bitrate = local.get("bitrate")

    r_codec = remote.get("codec")
    l_codec = local.get("codec")

    res_mode = comparison_settings.get("resolution_filter_mode", "none")
    use_res = (res_mode != "none")

    use_bitrate = bool(comparison_settings.get("use_bitrate", False))
    min_bitrate_diff_pct = float(comparison_settings.get("min_bitrate_diff_pct", 10.0))
    ignore_transcoded = bool(comparison_settings.get("ignore_transcoded", False))

    is_hls = str(remote.get("file", "")).lower().endswith(".m3u8")
    remote_transcoded = bool(
        remote.get("is_transcoded")
        or remote.get("transcoded")
        or remote.get("isTranscoded")
        or is_hls
    )

    # ── 1. Résolution ────────────────────────────────────────────────────────
    rr = resolution_rank(r_res) if r_res is not None else 0
    lr = resolution_rank(l_res) if l_res is not None else 0

    if ignore_transcoded and remote_transcoded:
        return {
            "blocked_by_transcode": True,
            "res_better": None,
            "res_rank_remote": rr,
            "res_rank_local": lr,
            "codec_better": None,
            "bitrate_better": None,
            "bitrate_pct": None,
            "include_item": False,
        }

    res_better = None
    if r_res is not None and l_res is not None:
        if rr > lr:
            res_better = True
        elif rr < lr:
            res_better = False
        else:
            res_better = None  # égal

    # ── 2. Codec ─────────────────────────────────────────────────────────────
    rc = codec_rank(r_codec)
    lc = codec_rank(l_codec)

    codec_better = None
    if r_codec and l_codec:
        if rc > lc:
            codec_better = True   # distant meilleur codec → intéressant
        elif rc < lc:
            codec_better = False  # local meilleur codec → on skip
        else:
            codec_better = None   # égal ou inconnu

    # Si le local a un codec strictement supérieur → skip immédiat
    # (ex : local AV1 vs distant H265 : peu importe la résolution/bitrate)
    if codec_better is False:
        return {
            "blocked_by_transcode": False,
            "res_better": res_better,
            "res_rank_remote": rr,
            "res_rank_local": lr,
            "codec_better": codec_better,
            "bitrate_better": None,
            "bitrate_pct": None,
            "include_item": False,
        }

    # ── 3. Bitrate ───────────────────────────────────────────────────────────
    bitrate_pct = None
    bitrate_better = None
    if use_bitrate and r_bitrate and l_bitrate and l_bitrate > 0:
        bitrate_pct = ((r_bitrate - l_bitrate) / l_bitrate) * 100.0
        bitrate_better = (bitrate_pct >= min_bitrate_diff_pct)

    # ── Décision finale ──────────────────────────────────────────────────────
    # Un item est inclus si le distant apporte quelque chose de mieux :
    # résolution supérieure OU codec supérieur OU (bitrate supérieur si activé).
    # Le filtre de résolution est appliqué côté client (JS) pour être instantané.
    has_upgrade = (res_better is True) or (codec_better is True) or (bitrate_better is True)

    if bitrate_better is not None:
        include_item = bitrate_better
    else:
        include_item = has_upgrade if (res_better is not None or codec_better is not None) else True

    return {
        "blocked_by_transcode": False,
        "res_better": res_better,
        "res_rank_remote": rr,
        "res_rank_local": lr,
        "codec_better": codec_better,
        "bitrate_better": bitrate_better,
        "bitrate_pct": bitrate_pct,
        "include_item": include_item,
    }


# =========================================================
# Matching Plex ↔ Plex
# =========================================================

def movie_match_score(remote_item, local_item):
    remote_title = remote_item.get("title") or ""
    local_title = local_item.get("title") or ""

    remote_norm = normalize_name(remote_title)
    local_norm = normalize_name(local_title)

    remote_tokens = title_tokens(remote_title)
    local_tokens = title_tokens(local_title)

    if not remote_tokens or not local_tokens:
        return {
            "score": 0,
            "containment": 0.0,
            "similarity": 0.0,
            "common_tokens": [],
        }

    common = sorted(set(remote_tokens) & set(local_tokens))
    containment = token_containment(remote_tokens, local_tokens)
    similarity = token_set_similarity(remote_tokens, local_tokens)

    score = 0

    if remote_norm == local_norm:
        score += 160

    if set(remote_tokens).issubset(set(local_tokens)):
        score += 100

    score += int(containment * 90)
    score += int(similarity * 50)

    remote_year = remote_item.get("year")
    local_year = local_item.get("year")

    if remote_year and local_year:
        try:
            diff = abs(int(remote_year) - int(local_year))
            if diff == 0:
                score += 35
            elif diff == 1:
                score += 15
            else:
                score -= 80
        except Exception:
            pass

    return {
        "score": score,
        "containment": containment,
        "similarity": similarity,
        "common_tokens": common,
    }


def show_match_score(remote_item, local_item):
    remote_title = remote_item.get("title") or ""
    local_title = local_item.get("title") or ""

    remote_norm = normalize_series_name(remote_title)
    local_norm = normalize_series_name(local_title)

    remote_tokens = series_tokens(remote_title)
    local_tokens = series_tokens(local_title)

    if not remote_tokens or not local_tokens:
        return {
            "score": 0,
            "containment": 0.0,
            "similarity": 0.0,
            "common_tokens": [],
        }

    common = sorted(set(remote_tokens) & set(local_tokens))
    containment = token_containment(remote_tokens, local_tokens)
    similarity = token_set_similarity(remote_tokens, local_tokens)

    score = 0

    if remote_norm == local_norm:
        score += 170

    if set(remote_tokens).issubset(set(local_tokens)):
        score += 100

    score += int(containment * 90)
    score += int(similarity * 50)

    # tolérance titres courts
    if len(set(remote_tokens)) <= 2:
        if len(common) >= 1:
            score += 25
        if containment >= 0.5:
            score += 20

    remote_year = remote_item.get("year")
    local_year = local_item.get("year")

    if remote_year and local_year:
        try:
            diff = abs(int(remote_year) - int(local_year))
            if diff == 0:
                score += 15
            elif diff > 1:
                score -= 20
        except Exception:
            pass

    return {
        "score": score,
        "containment": containment,
        "similarity": similarity,
        "common_tokens": common,
    }


def find_best_local_movie_match(remote_movie, local_movies):
    best = None

    remote_tokens = title_tokens(remote_movie.get("title"))

    for local in local_movies:
        result = movie_match_score(remote_movie, local)

        min_common = 1 if len(remote_tokens) <= 2 else 2
        min_containment = 0.60

        if result["containment"] < min_containment:
            continue
        if len(result["common_tokens"]) < min_common:
            continue

        candidate = {
            "local": local,
            **result
        }

        if best is None or candidate["score"] > best["score"]:
            best = candidate

    return best


def find_best_local_show_match(remote_show, local_shows):
    best = None

    remote_tokens = series_tokens(remote_show.get("title"))

    for local in local_shows:
        result = show_match_score(remote_show, local)

        min_common = 1 if len(remote_tokens) <= 2 else 2
        min_containment = 0.50 if len(remote_tokens) <= 2 else 0.70

        if result["containment"] < min_containment:
            continue
        if len(result["common_tokens"]) < min_common:
            continue

        remote_count = _safe_int(remote_show.get("episode_count")) or 0
        local_count = _safe_int(local.get("episode_count")) or 0

        score = result["score"]

        if remote_count and local_count:
            if remote_count == local_count:
                score += 40
            else:
                diff = abs(remote_count - local_count)
                if diff <= 2:
                    score += 10
                else:
                    score -= 15

        candidate = {
            "local": local,
            "score": score,
            "containment": result["containment"],
            "similarity": result["similarity"],
            "common_tokens": result["common_tokens"],
        }

        if best is None or candidate["score"] > best["score"]:
            best = candidate

    return best


# =========================================================
# Épisodes
# =========================================================

def _episode_key(ep):
    if not isinstance(ep, dict):
        return None

    season = _safe_int(ep.get("season"))
    episode = _safe_int(ep.get("episode"))

    if season is None or episode is None:
        return None

    return (season, episode)


def _season_counts(show_item) -> dict:
    """Retourne {numéro_saison: nb_épisodes} pour un item série."""
    counts = {}
    for ep in show_item.get("episodes") or []:
        s = _safe_int(ep.get("season"))
        if s is not None:
            counts[s] = counts.get(s, 0) + 1
    return counts


def build_episode_map(show_item):
    result = {}
    for ep in show_item.get("episodes") or []:
        key = _episode_key(ep)
        if key:
            result[key] = ep
    return result


def compute_missing_episodes(remote_show, local_show):
    remote_eps = build_episode_map(remote_show)
    local_eps = build_episode_map(local_show)

    missing = []

    for key in sorted(remote_eps.keys()):
        if key not in local_eps:
            ep = dict(remote_eps[key])
            ep.setdefault("title", f"S{key[0]:02d}E{key[1]:02d}")
            missing.append(ep)

    return missing


# =========================================================
# FTP enrichment
# =========================================================

def _enrich_movie_with_ftp(item):
    ftp_matches = find_ftp_matches_for_movie(
        item.get("title"),
        item.get("year")
    )
    best_match = ftp_matches[0] if ftp_matches else None
    best_ftp = best_match["item"] if best_match else None

    item["ftp_available"] = bool(best_ftp)
    item["ftp_item"] = best_ftp
    item["sources"] = ["plex", "ftp"] if best_ftp else ["plex"]
    item["ftp_confidence"] = best_match["confidence"] if best_match else None
    item["ftp_variant_type"] = best_match["variant_type"] if best_match else None
    return item


def _enrich_episode_with_ftp(show_title, ep, show_original_title=None):
    season = _safe_int(ep.get("season"))
    episode = _safe_int(ep.get("episode"))

    ftp_ep = None
    if season is not None and episode is not None:
        ftp_ep = find_ftp_match_for_episode(show_title, season, episode)
        # Si pas trouvé avec le titre principal, essayer avec l'originalTitle (cross-langue EN↔FR)
        if not ftp_ep and show_original_title and show_original_title != show_title:
            ftp_ep = find_ftp_match_for_episode(show_original_title, season, episode)

    ep_copy = dict(ep)
    ep_copy["ftp_available"] = bool(ftp_ep)
    ep_copy["ftp_item"] = ftp_ep
    ep_copy["ftp_confidence"] = 95 if ftp_ep else None
    ep_copy["ftp_variant_type"] = "episode_match" if ftp_ep else None
    ep_copy["sources"] = ["ftp"] if ftp_ep else []
    return ep_copy


def _enrich_episode_with_ftp_titles(search_titles, ep):
    """Variante de _enrich_episode_with_ftp acceptant une liste de titres pré-résolus."""
    season = _safe_int(ep.get("season"))
    episode = _safe_int(ep.get("episode"))

    ftp_ep = None
    if season is not None and episode is not None:
        for title in search_titles:
            ftp_ep = find_ftp_match_for_episode(title, season, episode)
            if ftp_ep:
                break

    ep_copy = dict(ep)
    ep_copy["ftp_available"] = bool(ftp_ep)
    ep_copy["ftp_item"] = ftp_ep
    ep_copy["ftp_confidence"] = 95 if ftp_ep else None
    ep_copy["ftp_variant_type"] = "episode_match" if ftp_ep else None
    ep_copy["sources"] = ["ftp"] if ftp_ep else []
    return ep_copy


# =========================================================
# Comparaisons unitaires
# =========================================================

def compare_movie(remote, local, settings):
    """
    Compare un film remote vs local.
    remote/local sont supposés déjà appariés.
    """
    try:
        if not _local_item_passes_label_filter(local, settings):
            return None

        r_res = remote.get("res")
        l_res = local.get("res")
        r_bitrate = remote.get("bitrate")
        l_bitrate = local.get("bitrate")
        r_codec = remote.get("codec")
        l_codec = local.get("codec")

        quality = _compute_quality_flags(remote, local, settings)

        if quality["blocked_by_transcode"]:
            logging.debug("[COMPARE_MOVIE] SKIP transcoded %s", remote.get("title"))
            return None

        if not quality["include_item"]:
            logging.debug(
                "[COMPARE_MOVIE] SKIP %s res R/L=%s/%s bitrate R/L=%s/%s pct=%s",
                remote.get("title"),
                r_res, l_res,
                r_bitrate, l_bitrate,
                quality["bitrate_pct"]
            )
            return None

        item = {
            "title": remote.get("title"),
            "year": remote.get("year"),
            "guid": remote.get("guid"),
            "my_res": l_res,
            "his_res": r_res,
            "my_codec": l_codec,
            "his_codec": r_codec,
            "my_bitrate": l_bitrate,
            "his_bitrate": r_bitrate,
            "res_rank_remote": quality["res_rank_remote"],
            "res_rank_local": quality["res_rank_local"],
            "type": "movie",
        }

        item = _enrich_movie_with_ftp(item)

        logging.debug(
            "[COMPARE_MOVIE] KEEP %s res R/L=%s/%s bitrate R/L=%s/%s pct=%s ftp=%s",
            remote.get("title"),
            r_res, l_res,
            r_bitrate, l_bitrate,
            quality["bitrate_pct"],
            item["ftp_available"]
        )

        return item

    except Exception as e:
        logging.error(f"Erreur dans compare_movie : {e}", exc_info=True)
        return None


def compare_show(remote, local, settings):
    """
    Compare une série remote vs local.
    remote/local sont supposés déjà appariés.
    """
    try:
        show_title = remote.get("title")
        show_original_title = remote.get("originalTitle") or None

        if not _local_item_passes_label_filter(local, settings):
            return None

        r_res = remote.get("res")
        l_res = local.get("res")
        r_bitrate = remote.get("bitrate")
        l_bitrate = local.get("bitrate")
        r_codec = remote.get("codec")
        l_codec = local.get("codec")

        # Option 2 : comparer les counts par saison avant le diff épisode-par-épisode
        # (coûteux). Si toutes les saisons ont le même nombre d'épisodes et que la
        # qualité ne justifie pas l'affichage, on skip immédiatement.
        # Plus fiable que le total seul : détecte les différences de spéciaux par saison.
        r_season_counts = _season_counts(remote)
        l_season_counts = _season_counts(local)
        seasons_match = bool(r_season_counts) and (r_season_counts == l_season_counts)

        quality = _compute_quality_flags(remote, local, settings)

        if quality["blocked_by_transcode"]:
            logging.debug("[COMPARE_SHOW] SKIP transcoded %s", remote.get("title"))
            return None

        if seasons_match and not quality["include_item"]:
            logging.debug(
                "[COMPARE_SHOW] SKIP (saisons identiques, qualité insuffisante) %s res R/L=%s/%s codec R/L=%s/%s bitrate R/L=%s/%s",
                remote.get("title"), r_res, l_res, r_codec, l_codec, r_bitrate, l_bitrate,
            )
            return None

        missing_episodes = []
        ftp_candidates = []

        # Pré-résoudre l'alias une seule fois pour toute la série
        from app.services.ftp_alias_service import resolve_alias
        try:
            from flask import current_app
            _api_key = current_app.config.get("TMDB_API_KEY") or ""
        except Exception:
            _api_key = ""
        _alias = resolve_alias(show_title, media_type="tv", api_key=_api_key)
        _search_titles = list(dict.fromkeys(filter(None, [show_title, _alias, show_original_title])))

        computed_missing = compute_missing_episodes(remote, local)
        for ep in computed_missing:
            enriched_ep = _enrich_episode_with_ftp_titles(_search_titles, ep)
            if enriched_ep.get("ftp_item"):
                ftp_candidates.append(enriched_ep["ftp_item"])
            missing_episodes.append(enriched_ep)

        # Si épisodes manquants, on affiche toujours.
        include_item = True if missing_episodes else quality["include_item"]

        if not include_item and not missing_episodes:
            logging.debug(
                "[COMPARE_SHOW] SKIP %s res R/L=%s/%s codec R/L=%s/%s bitrate R/L=%s/%s pct=%s",
                remote.get("title"),
                r_res, l_res,
                r_codec, l_codec,
                r_bitrate, l_bitrate,
                quality["bitrate_pct"]
            )
            return None

        ftp_available = bool(ftp_candidates)
        best_ftp = ftp_candidates[0] if ftp_candidates else None

        total_missing = len(missing_episodes) or 1
        total_ftp_found = len([ep for ep in missing_episodes if ep.get("ftp_available")])

        if total_ftp_found == 0:
            ftp_confidence = None
        else:
            ratio = total_ftp_found / total_missing
            if ratio >= 0.9:
                ftp_confidence = 95
            elif ratio >= 0.7:
                ftp_confidence = 85
            elif ratio >= 0.5:
                ftp_confidence = 75
            elif ratio >= 0.3:
                ftp_confidence = 65
            else:
                ftp_confidence = 55

        sources = ["plex"]
        if ftp_available:
            sources.append("ftp")

        item = {
            "title": remote.get("title"),
            "year": remote.get("year"),
            "guid": remote.get("guid"),
            "my_res": l_res,
            "his_res": r_res,
            "my_codec": l_codec,
            "his_codec": r_codec,
            "my_bitrate": l_bitrate,
            "his_bitrate": r_bitrate,
            "res_rank_remote": quality["res_rank_remote"],
            "res_rank_local": quality["res_rank_local"],
            "my_count": local.get("episode_count"),
            "his_count": remote.get("episode_count"),
            "missing_episodes": missing_episodes,
            "type": "show",
            "ftp_available": ftp_available,
            "ftp_item": best_ftp,
            "ftp_confidence": ftp_confidence,
            "ftp_variant_type": "episode_match" if ftp_available else None,
            "sources": sources,
        }

        logging.debug(
            "[COMPARE_SHOW] KEEP %s missing_eps=%s ftp=%s res R/L=%s/%s bitrate R/L=%s/%s pct=%s",
            remote.get("title"),
            len(missing_episodes),
            ftp_available,
            r_res, l_res,
            r_bitrate, l_bitrate,
            quality["bitrate_pct"]
        )

        return item

    except Exception as e:
        logging.error(f"Erreur dans compare_show : {e}", exc_info=True)
        return None


def compare_artist_albums(remote_item, local_item):
    """
    Compare les albums d’un artiste remote vs local.
    Retourne les albums manquants.
    """
    try:
        remote_albums = remote_item.get("albums", []) or []
        local_albums = local_item.get("albums", []) or []

        local_album_titles = {normalize_name(album.get("title", "")) for album in local_albums}

        missing_albums = []
        for remote_album in remote_albums:
            remote_album_title = remote_album.get("title", "")
            if normalize_name(remote_album_title) not in local_album_titles:
                missing_albums.append({
                    "title": remote_album_title,
                    "year": remote_album.get("year"),
                    "codec": remote_album.get("codec"),
                    "bitrate": remote_album.get("bitrate")
                })

        if missing_albums:
            return {
                "title": remote_item.get("title"),
                "type": "artist",
                "missing_albums": missing_albums
            }

        return None

    except Exception as e:
        logging.error(f"Erreur dans compare_artist_albums : {e}", exc_info=True)
        return None


# =========================================================
# Versions append-style (compatibilité avec ta logique Windows)
# =========================================================

def compare_and_append_movie(remote, local, comparison_list, comparison_settings):
    try:
        item = compare_movie(remote, local, comparison_settings)
        if item:
            comparison_list.append(item)
    except Exception as e:
        logging.error(f"Erreur dans compare_and_append_movie : {e}", exc_info=True)


def compare_and_append_show(remote, local, comparison_list, comparison_settings):
    try:
        item = compare_show(remote, local, comparison_settings)
        if item:
            comparison_list.append(item)
    except Exception as e:
        logging.error(f"Erreur dans compare_and_append_show : {e}", exc_info=True)


# =========================================================
# Helpers haut niveau pour faire le vrai Plex ↔ Plex
# =========================================================

def find_missing_and_better_movies(remote_movies, local_movies, comparison_settings):
    missing_items = []
    comparison_items = []

    for remote in remote_movies:
        best = find_best_local_movie_match(remote, local_movies)

        if not best:
            missing_item = {
                "title": remote.get("title"),
                "year": remote.get("year"),
                "guid": remote.get("guid"),
                "res": remote.get("res"),
                "codec": remote.get("codec"),
                "bitrate": remote.get("bitrate"),
                "type": "movie",
            }
            missing_item = _enrich_movie_with_ftp(missing_item)
            missing_items.append(missing_item)
            continue

        compare_and_append_movie(
            remote=remote,
            local=best["local"],
            comparison_list=comparison_items,
            comparison_settings=comparison_settings
        )

    return missing_items, comparison_items


def find_missing_and_better_shows(remote_shows, local_shows, comparison_settings):
    missing_items = []
    comparison_items = []

    for remote in remote_shows:
        best = find_best_local_show_match(remote, local_shows)

        if not best:
            # Série complètement absente localement
            all_remote_eps = []
            r_orig = remote.get("originalTitle") or None
            for ep in remote.get("episodes", []) or []:
                enriched_ep = _enrich_episode_with_ftp(remote.get("title"), ep, r_orig)
                all_remote_eps.append(enriched_ep)

            ftp_available = any(ep.get("ftp_available") for ep in all_remote_eps)
            best_ftp = next((ep.get("ftp_item") for ep in all_remote_eps if ep.get("ftp_item")), None)

            missing_items.append({
                "title": remote.get("title"),
                "year": remote.get("year"),
                "guid": remote.get("guid"),
                "episode_count": remote.get("episode_count"),
                "res": remote.get("res"),
                "codec": remote.get("codec"),
                "bitrate": remote.get("bitrate"),
                "type": "show",
                "missing_episodes": all_remote_eps,
                "ftp_available": ftp_available,
                "ftp_item": best_ftp,
                "ftp_confidence": 95 if ftp_available else None,
                "ftp_variant_type": "episode_match" if ftp_available else None,
                "sources": ["plex", "ftp"] if ftp_available else ["plex"],
            })
            continue

        compare_and_append_show(
            remote=remote,
            local=best["local"],
            comparison_list=comparison_items,
            comparison_settings=comparison_settings
        )

    return missing_items, comparison_items


def find_missing_artist_albums(remote_artists, local_artists):
    missing_items = []

    local_by_name = {
        normalize_name(item.get("title", "")): item
        for item in local_artists
    }

    for remote_artist in remote_artists:
        key = normalize_name(remote_artist.get("title", ""))
        local_artist = local_by_name.get(key)

        if not local_artist:
            # Artiste absent localement -> tous les albums sont manquants
            missing_albums = []
            for album in remote_artist.get("albums", []) or []:
                missing_albums.append({
                    "title": album.get("title"),
                    "year": album.get("year"),
                    "codec": album.get("codec"),
                    "bitrate": album.get("bitrate"),
                })

            if missing_albums:
                missing_items.append({
                    "title": remote_artist.get("title"),
                    "type": "artist",
                    "missing_albums": missing_albums
                })
            continue

        compared = compare_artist_albums(remote_artist, local_artist)
        if compared:
            missing_items.append(compared)

    return missing_items