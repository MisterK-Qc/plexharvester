import os
import ftplib
from flask import current_app


def get_ftp_client(host, port=21, username="", password="", use_tls=False, passive=True, timeout=30):
    if use_tls:
        ftp = ftplib.FTP_TLS()
        ftp.connect(host, int(port), timeout=timeout)
        ftp.login(username, password)
        ftp.prot_p()
    else:
        ftp = ftplib.FTP()
        ftp.connect(host, int(port), timeout=timeout)
        ftp.login(username, password)

    ftp.set_pasv(passive)
    return ftp


def ftp_list_dir(ftp, path):
    entries = []

    def parser(line):
        entries.append(line)

    ftp.retrlines(f"LIST {path}", parser)
    return entries


def ftp_walk_recursive(ftp, base_dir, is_video_file_func=None, status_dict=None):
    results = []

    def walk(current_dir):
        # Vérifier annulation avant chaque répertoire
        if status_dict is not None and status_dict.get("cancel_requested"):
            return

        try:
            current_app.logger.debug(f"[FTP] Scan dir: {current_dir}")
            entries = ftp_list_dir(ftp, current_dir)

            current_app.logger.debug(
                f"[FTP] {current_dir} -> {len(entries)} entrées"
            )

            for line in entries:
                parts = line.split(maxsplit=8)
                if len(parts) < 9:
                    current_app.logger.warning(
                        f"[FTP] Ligne LIST ignorée (parse impossible) dans {current_dir}: {line}"
                    )
                    continue

                perms = parts[0]
                size = parts[4]
                name = parts[8]

                if name in [".", ".."]:
                    continue

                full_path = f"{current_dir.rstrip('/')}/{name}"

                if perms.startswith("d"):
                    walk(full_path)
                else:
                    is_video = True
                    if is_video_file_func:
                        is_video = is_video_file_func(name)

                    if is_video:
                        item = {
                            "path": full_path,
                            "name": name,
                            "size": int(size) if str(size).isdigit() else 0
                        }
                        results.append(item)

                        if status_dict is not None:
                            status_dict["files_found"] = status_dict.get("files_found", 0) + 1

                            est_total = status_dict.get("estimated_total_files")
                            if est_total and est_total > 0:
                                percent = int((status_dict["files_found"] / est_total) * 100)
                                status_dict["estimated_percent"] = min(99, percent)
                            else:
                                status_dict["estimated_percent"] = None

        except Exception as e:
            current_app.logger.exception(
                f"[FTP] Erreur pendant le scan du dossier {current_dir}: {e}"
            )
            return

    walk(base_dir)
    return results

def ftp_file_size(ftp, remote_path):
    try:
        size = ftp.size(remote_path)
        return int(size) if size is not None else 0
    except Exception:
        return 0


def ftp_download_file(ftp, remote_path, local_path, progress_callback=None, blocksize=1024 * 512):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    temp_path = local_path + ".part"

    total_size = ftp_file_size(ftp, remote_path)
    downloaded = 0

    try:
        with open(temp_path, "wb") as f:
            def callback(chunk):
                nonlocal downloaded
                f.write(chunk)
                downloaded += len(chunk)

                if progress_callback:
                    percent = round((downloaded / total_size) * 100, 1) if total_size else 0
                    progress_callback(downloaded, total_size, percent)

            ftp.retrbinary(f"RETR {remote_path}", callback, blocksize=blocksize)

        os.replace(temp_path, local_path)
        return local_path

    except Exception:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass
        raise