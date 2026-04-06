from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify, current_app

from ..services.plex_snapshot_service import (
    create_snapshot,
    list_snapshots,
    load_snapshot,
    delete_snapshot,
    diff_snapshots,
)

snapshot_bp = Blueprint("snapshot", __name__, url_prefix="/snapshots")


@snapshot_bp.route("/")
def snapshots_page():
    if not session.get("logged_in"):
        return redirect(url_for("auth.login"))
    snapshots = list_snapshots()
    return render_template("snapshots.html", snapshots=snapshots)


@snapshot_bp.route("/create", methods=["POST"])
def create():
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401

    plex_token = session.get("plex_token")
    if not plex_token:
        return jsonify({"ok": False, "error": "Token Plex manquant"}), 401

    label = (request.get_json(silent=True) or {}).get("label", "")

    try:
        filename = create_snapshot(plex_token, label=label)
        snapshots = list_snapshots()
        snap_meta = next((s for s in snapshots if s["filename"] == filename), {})
        return jsonify({"ok": True, "filename": filename, "meta": snap_meta})
    except Exception as e:
        current_app.logger.exception("[SNAPSHOT] Erreur création")
        return jsonify({"ok": False, "error": str(e)}), 500


@snapshot_bp.route("/delete", methods=["POST"])
def delete():
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401

    filename = (request.get_json(silent=True) or {}).get("filename", "")
    try:
        ok = delete_snapshot(filename)
        return jsonify({"ok": ok})
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@snapshot_bp.route("/diff")
def diff():
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Non connecté"}), 401

    ref_file = request.args.get("ref", "")
    cur_file = request.args.get("current", "")

    if not ref_file or not cur_file:
        return jsonify({"ok": False, "error": "Paramètres ref et current requis"}), 400

    try:
        snap_ref = load_snapshot(ref_file)
        snap_cur = load_snapshot(cur_file)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    if not snap_ref:
        return jsonify({"ok": False, "error": f"Snapshot introuvable : {ref_file}"}), 404
    if not snap_cur:
        return jsonify({"ok": False, "error": f"Snapshot introuvable : {cur_file}"}), 404

    result = diff_snapshots(snap_ref, snap_cur)
    return jsonify({"ok": True, **result})
