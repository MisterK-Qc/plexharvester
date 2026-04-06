import logging
import requests

SERVICE_EQUIVALENTS = {
    "acorntv": {"acorntv", "acorntvamazonchannel"},
    "amazonprimevideo": {"primevideo", "amazon", "amazonprime", "amazonprimevideo"},
    "amcplus": {"amcplus", "amcplusamazonchannel"},
    "appletvplus": {"appletvplus", "appletv", "apple", "appletvchannel"},
    "britbox": {"britbox", "britboxamazonchannel"},
    "citytvplus": {"citytvplus", "citytvplusamazonchannel"},
    "clubillico": {"clubillico", "illicoplus"},
    "crave": {"crave", "cravetv"},
    "crunchyroll": {"crunchyroll", "crunchyrollamazonchannel"},
    "discoveryplus": {"discoveryplus", "discoveryplusamazonchannel"},
    "disneyplus": {"disneyplus"},
    "hidive": {"hidive", "hidiveamazonchannel"},
    "hollywoodsuite": {"hollywoodsuite", "hollywoodsuiteamazonchannel"},
    "icitoutv": {"icitoutv", "toutv"},
    "ifcfilmsunlimited": {"ifcfilmsunlimited", "ifcfilmsunlimitedamazonchannel"},
    "mgm": {"mgm", "mgmamazonchannel"},
    "netflix": {"netflix", "netflixkids"},
    "outtv": {"outtv", "outtvamazonchannel"},
    "paramountplus": {"paramountplus", "paramountpluspremium", "paramountplusamazonchannel", "paramountplusappletvchannel"},
    "shudder": {"shudder", "shudderamazonchannel"},
    "stacktv": {"stacktv", "stacktvamazonchannel"},
    "starz": {"starz", "starzamazonchannel"},
    "sundancenow": {"sundancenow", "sundancenowamazonchannel"},
    "superchannelplus": {"superchannelplus", "superchannelamazonchannel"},
    "teletoonplus": {"teletoonplus", "teletoonplusamazonchannel"},
    "tubitv": {"tubi", "tubitv"},
}

def safe_tmdb_get(url, params, timeout=15):
    variants = [
        dict(params),
        {**params, "language": "fr-CA"},
        {k: v for k, v in params.items() if k != "language"},
    ]

    last_error = None

    for variant in variants:
        try:
            r = requests.get(url, params=variant, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            logging.debug(f"[TMDB] retry failed for {url} with {variant}: {e}")
            last_error = e

    raise last_error

def normalize_service_name(name):
    if not name:
        return ""
    name = name.lower().replace(" ", "").replace("+", "plus")
    for canonical, variants in SERVICE_EQUIVALENTS.items():
        if name in variants:
            return canonical
    return name


def find_streaming_offers_tmdb(api_key, title, year=0, media_type='movie'):
    try:
        if not api_key or not title:
            return {}, {}, None, None

        is_tv = media_type == "tv"

        search_url = f"https://api.themoviedb.org/3/search/{media_type}"
        search_params = {
            "api_key": api_key,
            "query": title,
            "language": "fr-CA"
        }
        if year:
            if is_tv:
                search_params["first_air_date_year"] = year
            else:
                search_params["year"] = year

        search_response = safe_tmdb_get(search_url, search_params, timeout=15)
        results = search_response.json().get("results", [])
        if not results:
            return {}, {}, None, None

        tmdb_id = results[0]["id"]

        tmdb_total_episodes = None
        if is_tv:
            try:
                details_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}"
                details_resp = safe_tmdb_get(
                    details_url,
                    {"api_key": api_key, "language": "fr-CA"},
                    timeout=15
                )
                details = details_resp.json()
                v = details.get("number_of_episodes")
                if v is not None:
                    tmdb_total_episodes = int(v)
            except Exception as e:
                logging.warning(f"[TMDB] impossible de lire number_of_episodes pour '{title}': {e}")

        provider_url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/watch/providers"
        provider_response = safe_tmdb_get(
            provider_url,
            {"api_key": api_key},
            timeout=15
        )
        data = provider_response.json()

        ca_data = data.get("results", {}).get("CA", {})
        if not ca_data:
            return {}, {}, tmdb_id, tmdb_total_episodes

        base_link = ca_data.get("link", "")

        def build_provider_dict(providers):
            entries = {}
            grouped = {}

            for p in providers:
                name = p.get("provider_name")
                logo_path = p.get("logo_path")
                if not name or "with ads" in name.lower():
                    continue

                normalized_check = name.lower().replace(" ", "").replace("+", "plus")
                canonical_key = None
                for canonical, variants in SERVICE_EQUIVALENTS.items():
                    if normalized_check in variants:
                        canonical_key = canonical
                        break

                group_key = canonical_key if canonical_key else name
                grouped.setdefault(group_key, []).append({
                    "name": name,
                    "logo": f"https://image.tmdb.org/t/p/w45{logo_path}" if logo_path else None
                })

            final_providers = {}
            for group_key, provider_list in grouped.items():
                best_provider = provider_list[0]
                if len(provider_list) > 1:
                    for provider in provider_list:
                        lname = provider["name"].lower()
                        if "amazon channel" in lname or "apple tv channel" in lname:
                            best_provider = provider
                            break
                final_providers[group_key] = best_provider

            for provider_data in final_providers.values():
                entries[provider_data["name"]] = {
                    "link": base_link,
                    "logo": provider_data["logo"],
                    "tmdb_total_episodes": tmdb_total_episodes
                }

            return entries

        stream_availability = build_provider_dict(ca_data.get("flatrate", []))
        purchase_availability = build_provider_dict(ca_data.get("buy", []) + ca_data.get("rent", []))

        return stream_availability, purchase_availability, tmdb_id, tmdb_total_episodes

    except Exception as e:
        logging.error(f"[TMDB] erreur pour '{title}': {e}", exc_info=True)
        return {}, {}, None, None