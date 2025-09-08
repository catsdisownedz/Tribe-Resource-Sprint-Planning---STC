# routes/api.py
from flask import Blueprint, request, jsonify, send_file
from io import BytesIO
from datetime import datetime
import pandas as pd
from sqlalchemy import text
from db import fetch_one, fetch_all, execute, engine, get_current_qid

bp = Blueprint("api", __name__)

# ---------- helpers ----------

def _has_table_api(table: str) -> bool:
    return bool(fetch_one("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='public' AND table_name=:t
        LIMIT 1
    """, t=table))

def _has_col_api(table: str, col: str) -> bool:
    return bool(fetch_one("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name=:t AND column_name=:c
        LIMIT 1
    """, t=table, c=col))

def _ensure_master_assignments_shape_api():
    """Minimal shape needed by /api/assignments."""
    if not _has_table_api("master_assignments"):
        execute("""
            CREATE TABLE IF NOT EXISTS master_assignments (
              id SERIAL PRIMARY KEY,
              quarter_id INT NOT NULL REFERENCES quarters(id) ON DELETE CASCADE,
              tribe_name TEXT,
              app_name TEXT,
              resource_name TEXT,
              role TEXT,
              assignment_type TEXT,
              s1 BOOLEAN NOT NULL DEFAULT FALSE,
              s2 BOOLEAN NOT NULL DEFAULT FALSE,
              s3 BOOLEAN NOT NULL DEFAULT FALSE,
              s4 BOOLEAN NOT NULL DEFAULT FALSE,
              s5 BOOLEAN NOT NULL DEFAULT FALSE,
              s6 BOOLEAN NOT NULL DEFAULT FALSE,
              edited BOOLEAN NOT NULL DEFAULT FALSE,
              updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)
    else:
        for need_col, ddl in [
            ("quarter_id",      "INT REFERENCES quarters(id) ON DELETE CASCADE"),
            ("tribe_name",      "TEXT"),
            ("app_name",        "TEXT"),
            ("resource_name",   "TEXT"),
            ("role",            "TEXT"),
            ("assignment_type", "TEXT"),
            ("s1",              "BOOLEAN NOT NULL DEFAULT FALSE"),
            ("s2",              "BOOLEAN NOT NULL DEFAULT FALSE"),
            ("s3",              "BOOLEAN NOT NULL DEFAULT FALSE"),
            ("s4",              "BOOLEAN NOT NULL DEFAULT FALSE"),
            ("s5",              "BOOLEAN NOT NULL DEFAULT FALSE"),
            ("s6",              "BOOLEAN NOT NULL DEFAULT FALSE"),
            ("edited",          "BOOLEAN NOT NULL DEFAULT FALSE"),
            ("updated_at",      "TIMESTAMP NOT NULL DEFAULT NOW()"),
        ]:
            if not _has_col_api("master_assignments", need_col):
                execute(f"ALTER TABLE master_assignments ADD COLUMN {need_col} {ddl}")

    # Cheap index if missing
    try:
        execute("CREATE INDEX IF NOT EXISTS idx_ma_qid_api ON master_assignments(quarter_id)")
    except Exception:
        pass
      
      
def _row_to_dict(r):
    m = getattr(r, "_mapping", None)
    return dict(m) if m is not None else dict(r)

def _dicts(rows):
    return [_row_to_dict(r) for r in rows]

def sprint_cols():
    return ["s1","s2","s3","s4","s5","s6"]

def current_quarter_id():
    qid = get_current_qid()
    if not qid:
        raise RuntimeError("No current quarter configured")
    return int(qid)

def ilike_clause(col, val):
    val = (val or "").strip()
    if not val:
        return "", {}
    return f" AND {col} ILIKE :{col}_q ", {f"{col}_q": f"%{val}%"}

# ---------- catalog endpoints ----------
@bp.get("/tribes")
def list_tribes():
    qid = current_quarter_id()
    rows = fetch_all("""
        SELECT DISTINCT t.id, t.name
        FROM temp_assignments ta
        JOIN tribes t ON t.id = ta.tribe_id
        WHERE ta.quarter_id = :qid
        ORDER BY t.name
    """, qid=qid)
    return jsonify(_dicts(rows))

@bp.get("/resources")
def list_resources():
    """Optionally filter by tribe_id or tribe_name (one or the other)."""
    qid = current_quarter_id()
    tribe_id = request.args.get("tribe_id", type=int)
    tribe_name = (request.args.get("tribe_name") or "").strip()
    if tribe_id and tribe_name:
        return jsonify({"error": "Send only one of tribe_id or tribe_name"}), 400

    if tribe_id:
        rows = fetch_all("""
          SELECT DISTINCT r.id, r.name
          FROM temp_assignments ta
          JOIN resources r ON r.id = ta.resource_id
          WHERE ta.quarter_id = :qid AND ta.tribe_id = :tid
          ORDER BY r.name
        """, qid=qid, tid=tribe_id)
        return jsonify(_dicts(rows))

    if tribe_name:
        rows = fetch_all("""
          SELECT DISTINCT r.id, r.name
          FROM temp_assignments ta
          JOIN tribes t   ON t.id   = ta.tribe_id
          JOIN resources r ON r.id  = ta.resource_id
          WHERE ta.quarter_id = :qid AND t.name = :tname
          ORDER BY r.name
        """, qid=qid, tname=tribe_name)
        return jsonify(_dicts(rows))

    rows = fetch_all("""
      SELECT id, name
      FROM resources
      ORDER BY name
    """)
    return jsonify(_dicts(rows))

@bp.get("/tribes-for-resource")
def tribes_for_resource():
    qid = current_quarter_id()
    rid = request.args.get("resource_id", type=int)
    if not rid:
        return jsonify({"error":"resource_id required"}), 400
    rows = fetch_all("""
      SELECT DISTINCT t.name AS tribe_name
      FROM temp_assignments ta
      JOIN tribes t ON t.id = ta.tribe_id
      WHERE ta.quarter_id = :qid AND ta.resource_id = :rid
      ORDER BY t.name
    """, qid=qid, rid=rid)
    return jsonify([r["tribe_name"] for r in rows])

# ---------- availability (USED BY booking + edit) ----------
@bp.get("/availability")
def availability():
    """
    Supports two query shapes:

    1) Booking detail (temp flow):
       /api/availability?tribe=<name>&resource_id=<id>

    2) Edit flow (view bookings):
       /api/availability?tribe=<name>&resource_name=<text>[&role=<text>]

    Returns JSON:
      {
        resource_id, resource_name, tribe,
        assign_type, max_for_tribe,
        blocked: [0/1]*6,
        booked_by_tribe, remaining,
        # back-compat aliases used by some frontends:
        sprints: [0/1]*6,
        cap_per_tribe, shared_cap
      }
    """
    qid = current_quarter_id()
    tribe_name  = (request.args.get("tribe") or "").strip()
    rid         = request.args.get("resource_id", type=int)
    resource_nm = (request.args.get("resource_name") or "").strip()
    role        = (request.args.get("role") or "").strip()

    if not tribe_name:
        return jsonify({"error": "tribe is required"}), 400

    # ---- Resolve a single temp row for (tribe, resource) in this quarter ----
    temp = None
    if rid:
        temp = fetch_one("""
          SELECT
            r.id                 AS resource_id,
            r.name               AS resource_name,
            COALESCE(t.name, ta.tribe_name) AS tribe_name,
            ta.assign_type       AS assign_type,
            ta.reserved_sprints  AS reserved_sprints
          FROM temp_assignments ta
          JOIN resources r ON r.id = ta.resource_id
          LEFT JOIN tribes  t ON t.id = ta.tribe_id
          WHERE ta.quarter_id = :qid
            AND r.id = :rid
            AND (t.name = :tname OR ta.tribe_name = :tname)
          LIMIT 1
        """, qid=qid, rid=rid, tname=tribe_name)
    elif resource_nm:
        # map (resource_name [+ role]) -> resource id
        res = (fetch_one("""
                  SELECT id, name FROM resources
                  WHERE name = :rname AND role = :role
                  LIMIT 1
               """, rname=resource_nm, role=role) if role else
               fetch_one("""SELECT id, name FROM resources WHERE name = :rname LIMIT 1""",
                         rname=resource_nm))
        if not res:
            return jsonify({"error": "resource not found"}), 404
        rid = int(res["id"])

        temp = fetch_one("""
          SELECT
            r.id                 AS resource_id,
            r.name               AS resource_name,
            COALESCE(t.name, ta.tribe_name) AS tribe_name,
            ta.assign_type       AS assign_type,
            ta.reserved_sprints  AS reserved_sprints
          FROM temp_assignments ta
          JOIN resources r ON r.id = ta.resource_id
          LEFT JOIN tribes  t ON t.id = ta.tribe_id
          WHERE ta.quarter_id = :qid
            AND r.id = :rid
            AND (t.name = :tname OR ta.tribe_name = :tname)
          LIMIT 1
        """, qid=qid, rid=rid, tname=tribe_name)
    else:
        return jsonify({"error": "resource_id or resource_name is required"}), 400

    if not temp:
        return jsonify({"error": "This resource is not reserved for the selected tribe."}), 400

    resource_id   = int(temp["resource_id"])
    resource_name = temp["resource_name"]
    assign_type   = temp["assign_type"] or "Shared"
    # Cap per this tribe comes directly from the TEMP row
    max_for_tribe = int((temp.get("reserved_sprints") or 0))

    # ---- Blocked sprints by ANY tribe on this resource (booleans) ----
    agg = fetch_one("""
      SELECT
        COALESCE(MAX(CASE WHEN s1 IS TRUE THEN 1 ELSE 0 END),0) AS s1,
        COALESCE(MAX(CASE WHEN s2 IS TRUE THEN 1 ELSE 0 END),0) AS s2,
        COALESCE(MAX(CASE WHEN s3 IS TRUE THEN 1 ELSE 0 END),0) AS s3,
        COALESCE(MAX(CASE WHEN s4 IS TRUE THEN 1 ELSE 0 END),0) AS s4,
        COALESCE(MAX(CASE WHEN s5 IS TRUE THEN 1 ELSE 0 END),0) AS s5,
        COALESCE(MAX(CASE WHEN s6 IS TRUE THEN 1 ELSE 0 END),0) AS s6
      FROM master_assignments
      WHERE quarter_id = :qid AND resource_name = :rname
    """, qid=qid, rname=resource_name) or {c: 0 for c in sprint_cols()}
    blocked = [int(bool(agg[c])) for c in sprint_cols()]

        # ---- Sprints already held by THIS tribe (bitset) ----
    mine_row = fetch_one("""
      SELECT
        COALESCE(MAX(CASE WHEN s1 IS TRUE THEN 1 ELSE 0 END),0) AS s1,
        COALESCE(MAX(CASE WHEN s2 IS TRUE THEN 1 ELSE 0 END),0) AS s2,
        COALESCE(MAX(CASE WHEN s3 IS TRUE THEN 1 ELSE 0 END),0) AS s3,
        COALESCE(MAX(CASE WHEN s4 IS TRUE THEN 1 ELSE 0 END),0) AS s4,
        COALESCE(MAX(CASE WHEN s5 IS TRUE THEN 1 ELSE 0 END),0) AS s5,
        COALESCE(MAX(CASE WHEN s6 IS TRUE THEN 1 ELSE 0 END),0) AS s6
      FROM master_assignments
      WHERE quarter_id = :qid AND resource_name = :rname AND tribe_name = :tname
    """, qid=qid, rname=resource_name, tname=tribe_name) or {c:0 for c in sprint_cols()}
    mine = [int(bool(mine_row[c])) for c in sprint_cols()]

    # ---- Already booked by THIS tribe on this resource ----
    taken = fetch_one("""
      SELECT COALESCE(
        SUM( (CASE WHEN s1 THEN 1 ELSE 0 END) +
             (CASE WHEN s2 THEN 1 ELSE 0 END) +
             (CASE WHEN s3 THEN 1 ELSE 0 END) +
             (CASE WHEN s4 THEN 1 ELSE 0 END) +
             (CASE WHEN s5 THEN 1 ELSE 0 END) +
             (CASE WHEN s6 THEN 1 ELSE 0 END) ), 0) AS cnt
      FROM master_assignments
      WHERE quarter_id = :qid AND resource_name = :rname AND tribe_name = :tname
    """, qid=qid, rname=resource_name, tname=tribe_name)
    booked_by_tribe = int((taken or {}).get("cnt", 0))
    remaining = max(0, max_for_tribe - booked_by_tribe)

    # ---- Back-compat keys (some UI code expects these names) ----
    cap_per_tribe = max_for_tribe
    shared_cap    = max_for_tribe
    sprints       = blocked  # same data, older key name

    return jsonify({
        "resource_id": resource_id,
        "resource_name": resource_name,
        "tribe": tribe_name,
        "assign_type": assign_type,
        "max_for_tribe": max_for_tribe,
        "blocked": blocked,
        "mine": mine,               # <---- add this
        "booked_by_tribe": booked_by_tribe,
        "remaining": remaining,
        "sprints": sprints,
        "cap_per_tribe": cap_per_tribe,
        "shared_cap": shared_cap
    })


# ---------- assignments list ----------
@bp.get("/assignments")
def list_assignments():
    qid = current_quarter_id()
    #_ensure_master_assignments_shape_api()
    base = """
      SELECT id, tribe_name, app_name, resource_name, role, assignment_type,
             s1,s2,s3,s4,s5,s6, edited, updated_at
      FROM master_assignments
      WHERE quarter_id = :qid
    """
    q = base
    params = {"qid": qid}

    for key, col in [
        ("tribe","tribe_name"),("app","app_name"),
        ("resource","resource_name"),("role","role"),
        ("type","assignment_type")
    ]:
        clause, extra = ilike_clause(col, request.args.get(key,""))
        q += clause
        params.update(extra)

    q += " ORDER BY updated_at DESC NULLS LAST, id DESC"
    rows = fetch_all(q, **params)
    return jsonify(_dicts(rows))

# ---------- edit (PATCH) ----------
@bp.patch("/assignments/<int:aid>")
def patch_assignment(aid):
    """
    Partial update for s1..s6 in edit mode.
    Body: { s1..s6 (bool/0/1) }
    Enforces:
      - no clash with other tribes' booked sprints
      - per-tribe cap = reserved_sprints from temp_assignments for (tribe, resource) in current quarter
    """
    qid = current_quarter_id()
    data = request.get_json(force=True) or {}

    # Load the row we are editing
    row = fetch_one("""
      SELECT id, tribe_name, resource_name, assignment_type AS assign_type,
             s1,s2,s3,s4,s5,s6
      FROM master_assignments
      WHERE id = :id AND quarter_id = :qid
    """, id=aid, qid=qid)
    if not row:
        return jsonify({"error":"not found"}), 404

    tribe = row["tribe_name"]
    rname = row["resource_name"]
    asg_type = (row["assign_type"] or "Shared").strip() or "Shared"

    # normalize incoming booleans (final state)
    future = {}
    for i in range(1,7):
        k = f"s{i}"
        if k in data:
            v = data[k]
            if isinstance(v, bool): future[k] = v
            elif isinstance(v, (int,)): future[k] = bool(v)
            elif isinstance(v, str):
                vv = v.strip().lower()
                future[k] = vv in ("1","true","t","yes","y")
            else:
                future[k] = False
        else:
            # keep current value if not provided
            future[k] = bool(row[k])
        # --- Early exit: if no sprint values changed, skip update so 'edited' stays as-is ---
    if all(bool(row[f"s{i}"]) == future[f"s{i}"] for i in range(1,7)):
        return jsonify({"ok": True, "unchanged": True})


    # 1) Blocked by OTHER tribes on this resource (boolean-safe)
    blocked = fetch_one("""
      SELECT
        COALESCE(MAX(CASE WHEN s1 IS TRUE THEN 1 ELSE 0 END),0) AS s1,
        COALESCE(MAX(CASE WHEN s2 IS TRUE THEN 1 ELSE 0 END),0) AS s2,
        COALESCE(MAX(CASE WHEN s3 IS TRUE THEN 1 ELSE 0 END),0) AS s3,
        COALESCE(MAX(CASE WHEN s4 IS TRUE THEN 1 ELSE 0 END),0) AS s4,
        COALESCE(MAX(CASE WHEN s5 IS TRUE THEN 1 ELSE 0 END),0) AS s5,
        COALESCE(MAX(CASE WHEN s6 IS TRUE THEN 1 ELSE 0 END),0) AS s6
      FROM master_assignments
      WHERE quarter_id = :qid
        AND resource_name = :rname
        AND tribe_name <> :tname
    """, qid=qid, rname=rname, tname=tribe) or {f"s{i}": 0 for i in range(1,7)}

    # You can turn OFF a sprint even if others booked it; but you cannot turn ON if blocked by another tribe.
    bad = [i for i in range(1,7)
           if future[f"s{i}"] and int(blocked[f"s{i}"]) == 1]
    if bad:
        return jsonify({"error": f"Sprint(s) {', '.join(map(str, bad))} already booked by another tribe."}), 409

    # 2) Cap per tribe for this (tribe, resource), from temp_assignments.reserved_sprints
    #    (works whether temp stores tribe_id or tribe_name)
    rs = fetch_one("""
      SELECT ta.reserved_sprints
      FROM temp_assignments ta
      JOIN resources r ON r.id = ta.resource_id
      LEFT JOIN tribes   t ON t.id = ta.tribe_id
      WHERE ta.quarter_id = :qid
        AND r.name = :rname
        AND (t.name = :tname OR ta.tribe_name = :tname)
      ORDER BY ta.id DESC
      LIMIT 1
    """, qid=qid, rname=rname, tname=tribe)
    cap_per_tribe = int((rs or {}).get("reserved_sprints") or 0)
    # Dedicated safety (in case temp said 0 but assignment is Dedicated)
    if asg_type == "Dedicated":
        cap_per_tribe = max(cap_per_tribe, 6)

    # Count how many sprints will be ON after this edit
    future_on = sum(1 for i in range(1,7) if future[f"s{i}"])
    if future_on > cap_per_tribe:
        return jsonify({"error": f"you’ve exceeded max allowed number of sprints ({cap_per_tribe})."}), 400

    # 3) Apply the update
    cols = [f"{k} = :{k}" for k in [f"s{i}" for i in range(1,7)]]
    params = {"id": aid, "qid": qid, **future}
    query = "UPDATE master_assignments SET " + ",".join(cols) + \
            ", edited = TRUE, updated_at = NOW() WHERE id = :id AND quarter_id = :qid"
    execute(query, **params)
    return jsonify({"ok": True})

# ---------- export ----------
@bp.get("/export")
def export_assignments():
    qid = current_quarter_id()
    base = """
      SELECT tribe_name, app_name, resource_name, role, assignment_type,
             s1,s2,s3,s4,s5,s6, edited, updated_at
      FROM master_assignments
      WHERE quarter_id = :qid
    """
    params = {"qid": qid}
    for key, col in [
        ("tribe","tribe_name"),("app","app_name"),
        ("resource","resource_name"),("role","role"),
        ("type","assignment_type")
    ]:
        clause, extra = ilike_clause(col, request.args.get(key,""))
        base += clause
        params.update(extra)

    rows = fetch_all(base + " ORDER BY tribe_name, resource_name, role", **params)
    if not rows:
        return jsonify({"error":"nothing to export"}), 400

    df = pd.DataFrame(rows)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        df.to_excel(xw, sheet_name="assignments", index=False)
    buf.seek(0)
    fname = f"assignments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    bio = BytesIO(buf.read())
    bio.seek(0)
    return send_file(bio, as_attachment=True, download_name=fname,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ---------- booking (create new assignment row) ----------
@bp.post("/book")
def create_booking():
    """
    Body:
    {
      "tribe": "<tribe name>",
      "resource_id": <int>,
      "s1": bool, ..., "s6": bool
    }

    Creates (or merges with) a master_assignments row for the current quarter,
    respecting 'Dedicated'/'Shared' rules, boolean columns, and sprint caps.
    """
    qid = current_quarter_id()
    payload = request.get_json(force=True) or {}

    tribe_name = (payload.get("tribe") or "").strip()
    rid = payload.get("resource_id", None)

    if not tribe_name or not isinstance(rid, int):
        return jsonify({"error": "tribe (name) and resource_id are required"}), 400

    # Resolve resource_name and assign_type from temp_assignments for this tribe/resource
    temp = fetch_one("""
      SELECT r.name AS resource_name,
       r.role AS resource_role,
       ta.assign_type,
       ta.app_name
      FROM temp_assignments ta
      JOIN resources r ON r.id = ta.resource_id
      JOIN tribes    t ON t.id = ta.tribe_id
      WHERE ta.quarter_id = :qid AND t.name = :tname AND r.id = :rid
      LIMIT 1
    """, qid=qid, tname=tribe_name, rid=rid)
    if not temp:
        return jsonify({"error":"This resource is not reserved for the selected tribe."}), 400

    resource_name = temp["resource_name"]
    assign_type   = temp["assign_type"]

    # normalize incoming bools
    def to_bool(v):
        if isinstance(v, bool): return v
        if isinstance(v, (int,)): return bool(v)
        if isinstance(v, str): 
            v = v.strip().lower()
            if v in ("1","true","t","yes","y"): return True
            if v in ("0","false","f","no","n",""): return False
        return False

    future = { f"s{i}": to_bool(payload.get(f"s{i}", False)) for i in range(1,7) }
    requested_cnt = sum(1 for i in range(1,7) if future[f"s{i}"])

    # 1) Blocked sprints by other tribes on same resource (boolean-safe)
    blocked = fetch_one("""
      SELECT
        COALESCE(MAX(CASE WHEN s1 IS TRUE THEN 1 ELSE 0 END),0) AS s1,
        COALESCE(MAX(CASE WHEN s2 IS TRUE THEN 1 ELSE 0 END),0) AS s2,
        COALESCE(MAX(CASE WHEN s3 IS TRUE THEN 1 ELSE 0 END),0) AS s3,
        COALESCE(MAX(CASE WHEN s4 IS TRUE THEN 1 ELSE 0 END),0) AS s4,
        COALESCE(MAX(CASE WHEN s5 IS TRUE THEN 1 ELSE 0 END),0) AS s5,
        COALESCE(MAX(CASE WHEN s6 IS TRUE THEN 1 ELSE 0 END),0) AS s6
      FROM master_assignments
      WHERE quarter_id = :qid AND resource_name = :rname AND tribe_name <> :tname
    """, qid=qid, rname=resource_name, tname=tribe_name) or {f"s{i}": 0 for i in range(1,7)}

    bad = [i for i in range(1,7) if future[f"s{i}"] and int(blocked[f"s{i}"]) == 1]
    if bad:
        return jsonify({"error": f"Sprint(s) {', '.join(map(str, bad))} already booked by another tribe."}), 409

    # 2) Capacity per tribe on Shared vs Dedicated
    share = fetch_one("""
      SELECT COUNT(DISTINCT ta.tribe_id) AS n
      FROM temp_assignments ta
      WHERE ta.quarter_id = :qid AND ta.resource_id = :rid
    """, qid=qid, rid=rid)
    num_sharing = int(share["n"]) if share and share.get("n") else 1
    cap_per_tribe = 6 if assign_type == "Dedicated" else max(1, (6 // max(1, num_sharing)))

    # 3) What has THIS tribe already booked on this resource?
    already = fetch_one("""
      SELECT COALESCE(SUM(
        (CASE WHEN s1 IS TRUE THEN 1 ELSE 0 END) +
        (CASE WHEN s2 IS TRUE THEN 1 ELSE 0 END) +
        (CASE WHEN s3 IS TRUE THEN 1 ELSE 0 END) +
        (CASE WHEN s4 IS TRUE THEN 1 ELSE 0 END) +
        (CASE WHEN s5 IS TRUE THEN 1 ELSE 0 END) +
        (CASE WHEN s6 IS TRUE THEN 1 ELSE 0 END)
      ), 0) AS cnt
      FROM master_assignments
      WHERE quarter_id = :qid AND resource_name = :rname AND tribe_name = :tname
    """, qid=qid, rname=resource_name, tname=tribe_name)
    already_cnt = int((already or {}).get("cnt", 0))
    if already_cnt + requested_cnt > cap_per_tribe:
        return jsonify({"error": f"you’ve exceeded max allowed number of sprints ({cap_per_tribe})."}), 400

    # 4) Upsert row for this tribe/resource
    existing = fetch_one("""
      SELECT id
      FROM master_assignments
      WHERE quarter_id = :qid AND tribe_name = :tname AND resource_name = :rname
      ORDER BY id ASC
      LIMIT 1
    """, qid=qid, tname=tribe_name, rname=resource_name)

    params = {
        "qid": qid,
        "tname": tribe_name,
        "rname": resource_name,
        "rrole": temp["resource_role"],
        "aname": temp["app_name"],
        **future
    }

    if existing:
        execute("""
          UPDATE master_assignments
          SET s1=:s1, s2=:s2, s3=:s3, s4=:s4, s5=:s5, s6=:s6,
           updated_at = NOW()
          WHERE id = :id AND quarter_id = :qid
        """, **params, id=existing["id"])
        return jsonify({"ok": True, "id": existing["id"], "mode": "updated"})
    else:
        execute("""
          INSERT INTO master_assignments
            (quarter_id, tribe_name, app_name, resource_name, role, assignment_type,
            s1, s2, s3, s4, s5, s6, edited, updated_at)
          VALUES
            (:qid, :tname, :aname, :rname, :rrole, :atype,
            :s1, :s2, :s3, :s4, :s5, :s6, FALSE, NOW())
        """, **params, atype=assign_type)
        newrow = fetch_one("""
          SELECT id FROM master_assignments
          WHERE quarter_id = :qid AND tribe_name = :tname AND resource_name = :rname
          ORDER BY id DESC LIMIT 1
        """, qid=qid, tname=tribe_name, rname=resource_name)
        return jsonify({"ok": True, "id": int(newrow["id"]), "mode": "created"})
