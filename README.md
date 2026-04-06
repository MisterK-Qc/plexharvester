# PlexHarvester

**PlexHarvester** is a self-hosted web application that compares your local Plex library against shared remote servers, indexes FTP sources for automatic downloading, and manages MKV audio tracks — all from a clean web interface.

[![Docker Pulls](https://img.shields.io/docker/pulls/DOCKERHUB_USERNAME/plexharvester)](https://hub.docker.com/r/DOCKERHUB_USERNAME/plexharvester)
[![Docker Image Size](https://img.shields.io/docker/image-size/DOCKERHUB_USERNAME/plexharvester/latest)](https://hub.docker.com/r/DOCKERHUB_USERNAME/plexharvester)
[![GitHub release](https://img.shields.io/github/v/release/MisterK-Qc/plexharvester)](https://github.com/MisterK-Qc/plexharvester/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Features

- **Library Comparison** — Compare movies and TV shows between your local Plex server and any shared remote server, with smart cross-language title matching (e.g. EN vs FR titles)
- **FTP Integration** — Index multiple FTP servers, queue downloads automatically, and route content to the right Plex library folder
- **MKV Processing** — Remux video files with MKVToolNix to set the correct default audio track and fix language metadata per track
- **Snapshots** — Save library snapshots and diff them over time to track additions and removals
- **Multilingual** — Full French and English interface
- **Real-time Logs** — Live log viewer with SSE streaming and configurable retention
- **Auto-download scheduling** — Time-windowed automatic FTP downloads with daily limits and skip days

---

## Quick Start

### Docker Compose

```yaml
services:
  plexharvester:
    image: DOCKERHUB_USERNAME/plexharvester:latest
    container_name: plexharvester
    ports:
      - "5000:5000"
    volumes:
      - /mnt/user/appdata/plexharvester:/config
    environment:
      TZ: America/Montreal
      SECRET_KEY: "your-random-secret-key"
    restart: unless-stopped
```

Then open **http://YOUR_IP:5000** in your browser.

### Docker Run

```bash
docker run -d \
  --name plexharvester \
  -p 5000:5000 \
  -e SECRET_KEY="your-random-secret-key" \
  -e TZ="America/Montreal" \
  -v /mnt/user/appdata/plexharvester:/config \
  --restart unless-stopped \
  DOCKERHUB_USERNAME/plexharvester:latest
```

---

## Unraid Installation

### Option 1 — Community Applications (recommended)

1. In Unraid, open **Apps** (Community Applications)
2. Search for **PlexHarvester**
3. Click Install and fill in the required fields (see [Environment Variables](#environment-variables))

### Option 2 — Manual template

1. In Unraid, go to **Docker > Add Container**
2. Paste the following template URL in the **Template URL** field:
   ```
   https://raw.githubusercontent.com/MisterK-Qc/plexharvester/main/unraid-template/plexharvester.xml
   ```
3. Fill in the required fields and click **Apply**

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SECRET_KEY` | **Yes** | — | Flask secret key for session security. Use a long random string. |
| `TZ` | No | `America/Montreal` | Container timezone |
| `PLEX_COMPARE_CONFIG_DIR` | No | `/config` | Internal config directory (do not change) |

> **Generate a SECRET_KEY:**
> ```bash
> openssl rand -hex 32
> ```

---

## Volumes

| Container path | Description |
|---|---|
| `/config` | **Required.** Persistent storage for config, cache, and logs. |

---

## First Launch

1. Open **http://YOUR_IP:5000**
2. Log in with your **Plex account** — the token is retrieved automatically
3. Select the **remote server** to compare against
4. Go to **Config (⚙️)** to set up:
   - TMDB API key *(optional)*
   - FTP servers *(optional)*
   - Default audio language for MKV processing
   - Interface language (French / English)

---

## Configuration

All settings are managed through the web interface under **Config**. The configuration is stored in `/config/config.json`.

See [config.example.json](config.example.json) for a reference of all available options.

---

## MKV Processing

MKVToolNix is **included in the Docker image** — no separate installation required. The default binary path is `/usr/bin`.

Features:
- Auto-detect audio track languages from file metadata
- Set default audio track based on your preferred language
- Override language per track directly from the UI
- Add custom language codes to the dropdown list

---

## Architecture

Built with:
- **Python 3.12** + **Flask** — Web framework
- **PlexAPI** — Plex server communication
- **Gunicorn** — WSGI server (1 worker, 4 threads)
- **MKVToolNix** — MKV remuxing and metadata editing

```
app/
  routes/         Flask blueprints (dashboard, config, FTP, MKV, auth)
  services/       Business logic (Plex, FTP, MKV, cache, config)
  templates/      Jinja2 HTML templates
  languages/      i18n files (fr.json, en.json)
  static/         CSS, favicon
```

---

## Building from Source

```bash
git clone https://github.com/MisterK-Qc/plexharvester.git
cd plexharvester
docker build -t plexharvester:latest .
```

> Code changes in `app/` and `run.py` only require a container restart when using bind mounts — no rebuild needed.

---

## License

MIT — see [LICENSE](LICENSE)
