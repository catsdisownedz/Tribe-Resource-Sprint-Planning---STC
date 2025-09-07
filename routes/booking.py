from flask import Blueprint, request, jsonify, render_template
import os
from db import fetch_all, fetch_one, execute, get_current_qid

bp = Blueprint("booking", __name__)

def get_current_quarter_id():
    return get_current_qid()

def _build_filter_sql(params_in):
    clauses = ["ta.quarter_id = :qid"]
    params = {"qid": params_in["qid"]}

    if params_in.get("tribe"):
        clauses.append("ta.tribe_name ILIKE :tribe")
        params["tribe"] = f"%{params_in['tribe']}%"
    if params_in.get("type"):
        clauses.append("ta.assign_type ILIKE :type")
        params["type"] = f"%{params_in['type']}%"
    if params_in.get("app"):
        clauses.append("ta.app_name ILIKE :app")
        params["app"] = f"%{params_in['app']}%"
    if params_in.get("role"):
        clauses.append("r.role ILIKE :role")
        params["role"] = f"%{params_in['role']}%"
    if params_in.get("resource"):
        clauses.append("r.name ILIKE :resource")
        params["resource"] = f"%{params_in['resource']}%"

    where_sql = " AND ".join(clauses)
    return where_sql, params

@bp.get("/api/temp-assignments")
def temp_assignments_list():
    qid = get_current_quarter_id()
    if not qid:
        return jsonify({"error": "No quarter configured"}), 400

    where_sql, params = _build_filter_sql({
        "qid": qid,
        "tribe": request.args.get("tribe"),
        "type": request.args.get("type"),
        "app": request.args.get("app"),
        "role": request.args.get("role"),
        "resource": request.args.get("resource"),
    })

    rows = fetch_all(
        f"""
        SELECT
            ta.id           AS temp_id,
            ta.tribe_name   AS tribe,
            ta.assign_type  AS type,
            ta.app_name     AS app,
            r.name          AS resource_name,
            r.role          AS role,
            ta.resource_id  AS resource_id,
             ta.reserved_sprints AS reserved
        FROM temp_assignments ta
        JOIN resources r ON r.id = ta.resource_id
        WHERE {where_sql}
        ORDER BY ta.tribe_name, r.name
        """,
        **params,
    )

    return jsonify({"items": rows})

@bp.get("/booking/<int:temp_id>")
def booking_detail_page(temp_id):
    # Compute real BOOKED_SPRINTS and RESERVED_LIMIT for initial render
    qid = get_current_quarter_id()
    
    
    row = fetch_one("""
        SELECT COALESCE(name, code) AS title
        FROM quarters
        WHERE is_current = TRUE
        LIMIT 1
    """)
    current = (row or {}).get("title") or ""

    if not qid:
        return render_template(
            "booking_detail.html",
            temp_id=temp_id,
            booked_sprints=[],
            reserved_sprints=0,
            current_quarter=current,   # <-- add this
        )

    temp = fetch_one(
        """
        SELECT ta.id AS temp_id, ta.tribe_name, ta.assign_type, ta.app_name,
               ta.resource_id, r.name AS resource_name, r.role, ta.reserved_sprints
        FROM temp_assignments ta
        JOIN resources r ON r.id = ta.resource_id
        WHERE ta.id = :id AND ta.quarter_id = :qid
        """,
        id=temp_id,
        qid=qid,
    )
    if not temp:
        # Keep page shape, but show nothing blocked
        return render_template(
            "booking_detail.html",
            temp_id=temp_id,
            booked_sprints=[],
            reserved_sprints=0,
        )

    # Per-sprint capacity (applies to Shared)
    cap = int(os.getenv("SHARED_MAX_TRIBES_PER_SPRINT", "3"))

    # booking.py (inside booking_detail_page)
    master = fetch_all(
        """
        SELECT ma.tribe_name, ma.s1, ma.s2, ma.s3, ma.s4, ma.s5, ma.s6
        FROM master_assignments ma
        JOIN resources r
        ON r.id = :rid
        AND ma.resource_name = r.name
        WHERE ma.quarter_id = :qid
        """,
        qid=qid, rid=temp["resource_id"],
    )


    # Combine ALL tribes’ rows for that resource to get truly "booked" sprints
    booked_flags = [False]*6
    
    for row in master:
        for i, col in enumerate(("s1","s2","s3","s4","s5","s6")):
            v = row.get(col)
            if isinstance(v, bool):
                booked_flags[i] |= v
            elif v in (1, "1", "Y", "y", "T", "t", "true", "True"):
                booked_flags[i] = True

        # How many sprints are already booked by THIS tribe on this resource?
    mine_count_row = fetch_one("""
      SELECT COALESCE(
        SUM( (CASE WHEN s1 THEN 1 ELSE 0 END) +
             (CASE WHEN s2 THEN 1 ELSE 0 END) +
             (CASE WHEN s3 THEN 1 ELSE 0 END) +
             (CASE WHEN s4 THEN 1 ELSE 0 END) +
             (CASE WHEN s5 THEN 1 ELSE 0 END) +
             (CASE WHEN s6 THEN 1 ELSE 0 END) ), 0) AS cnt
      FROM master_assignments
      WHERE quarter_id = :qid AND resource_name = :rname AND tribe_name = :tname
    """, qid=qid, rname=temp["resource_name"], tname=temp["tribe_name"]) or {"cnt":0}
    booked_by_tribe = int(mine_count_row["cnt"])


    # Build counts and who booked each sprint
    counts = {i: 0 for i in range(1, 7)}
    booked_by = {i: [] for i in range(1, 7)}
    for row in master:
        for i, col in enumerate(["s1", "s2", "s3", "s4", "s5", "s6"], start=1):
            if bool(row[col]):
                counts[i] += 1
                booked_by[i].append(row["tribe_name"])

    # Decide which sprint indexes are blocked for this temp assignment
# Block a sprint if ANY tribe has it (same rule as the JSON API)
    booked_indices = [i for i, flag in enumerate(booked_flags, start=1) if flag]


    reserved = int(temp.get("reserved_sprints") or 0)

    booked_sprints = [i+1 for i, flag in enumerate(booked_flags) if flag]
    return render_template(
        "booking_detail.html",
        temp_id=temp_id,
        booked_sprints=booked_sprints,
        reserved_sprints=reserved,
        booked_by_tribe=booked_by_tribe,
        current_quarter=current,       # <-- add this
    )





@bp.get("/api/temp-assignments/<int:temp_id>")
def temp_assignment_detail(temp_id):
    qid = get_current_quarter_id()
    if not qid:
        return jsonify({"error": "No quarter configured"}), 400

    temp = fetch_one(
        """
        SELECT ta.id AS temp_id, ta.tribe_name, ta.assign_type, ta.app_name,
               ta.resource_id, r.name AS resource_name, r.role, ta.reserved_sprints
        FROM temp_assignments ta
        JOIN resources r ON r.id = ta.resource_id
        WHERE ta.id = :id AND ta.quarter_id = :qid
        """,
        id=temp_id,
        qid=qid,
    )

    if not temp:
        return jsonify({"error": "Temp assignment not found"}), 404

    # Allowed tribes
    if temp["assign_type"] == "Shared":
        tribe_rows = fetch_all(
            """
            SELECT DISTINCT tribe_name
            FROM temp_assignments
            WHERE quarter_id = :qid AND resource_id = :rid
            ORDER BY tribe_name
            """,
            qid=qid, rid=temp["resource_id"]
        )
        allowed_tribes = [t["tribe_name"] for t in tribe_rows]
    else:
        allowed_tribes = [temp["tribe_name"]]

    # Shared per-sprint capacity (default 3) — kept for sprint-level blocking
    cap = int(os.getenv("SHARED_MAX_TRIBES_PER_SPRINT", "3"))

    # master_assignments has resource_name, not resource_id
    master = fetch_all(
        """
        SELECT ma.tribe_name, ma.s1, ma.s2, ma.s3, ma.s4, ma.s5, ma.s6
        FROM master_assignments ma
        JOIN resources r
        ON r.id = :rid
        AND ma.resource_name = r.name
        WHERE ma.quarter_id = :qid
        """,
        qid=qid,
        rid=temp["resource_id"],
    )


    # Count bookings per sprint
    booked_by = {i: [] for i in range(1, 7)}
    counts = {i: 0 for i in range(1, 7)}
    for row in master:
        for i, col in enumerate(["s1", "s2", "s3", "s4", "s5", "s6"], start=1):
            if bool(row[col]):
                counts[i] += 1
                booked_by[i].append(row["tribe_name"])

    sprints = []
    for i in range(1, 7):
        # Treat a sprint as blocked if it’s already booked by anyone.
        blocked = counts[i] > 0
        sprints.append({
            "index": i,
            "blocked": bool(blocked),
            "taken_by": booked_by[i],
            "type": temp["assign_type"],
            "can_book": not blocked or (temp["assign_type"] == "Dedicated" and temp["tribe_name"] in booked_by[i])
        })

    return jsonify(
        {
            "temp": temp,
            "allowed_tribes": allowed_tribes,
            "sprints": sprints,
            "shared_cap": cap,
        }
    )

@bp.post("/api/book-temp/<int:temp_id>")
def book_temp(temp_id):
    qid = get_current_quarter_id()
    if not qid:
        return jsonify({"error": "No quarter configured"}), 400

    payload = request.get_json(force=True) or {}
    selected_sprints = payload.get("sprints", [])
    # normalize/validate sprint indices 1..6
    try:
        selected_sprints = sorted({int(s) for s in selected_sprints if 1 <= int(s) <= 6})
    except Exception:
        return jsonify({"error": "Invalid sprint indexes"}), 400

    # Load the temp row with all fields we need
    temp = fetch_one("""
        SELECT ta.id AS temp_id,
               ta.tribe_name,
               ta.assign_type,
               ta.app_name,
               ta.resource_id,
               r.name AS resource_name,
               r.role AS resource_role
        FROM temp_assignments ta
        JOIN resources r ON r.id = ta.resource_id
        WHERE ta.id = :id AND ta.quarter_id = :qid
        """, id=temp_id, qid=qid)
    if not temp:
        return jsonify({"error": "Temp assignment not found"}), 404

    tribe       = temp["tribe_name"]
    rid         = int(temp["resource_id"])
    resource    = temp["resource_name"]
    role        = temp["resource_role"]
    assign_type = (temp["assign_type"] or "Shared").strip() or "Shared"

    # master rows for THIS resource+role in THIS quarter (need tribe_name for per-tribe checks)
    master = fetch_all("""
        SELECT id, tribe_name, s1, s2, s3, s4, s5, s6
        FROM master_assignments
        WHERE quarter_id = :qid
          AND resource_name = :rname
          AND role = :rrole
        """, qid=qid, rname=resource, rrole=role)

    # Build per-sprint occupancy + whether THIS tribe already has it
    cap_shared = int(os.getenv("SHARED_MAX_TRIBES_PER_SPRINT", "3"))
    counts     = {i: 0 for i in range(1, 7)}
    tribe_has  = {i: False for i in range(1, 7)}
    for m in master:
        for i, col in enumerate(["s1","s2","s3","s4","s5","s6"], start=1):
            if bool(m[col]):
                counts[i] += 1
                if m["tribe_name"] == tribe:
                    tribe_has[i] = True

    # sprint-level blocking
    errors = []
    for s in selected_sprints:
        if assign_type == "Dedicated":
            if counts[s] > 0 and not tribe_has[s]:
                errors.append(f"Sprint S{s} already taken by another tribe")
        else:
            if counts[s] >= cap_shared and not tribe_has[s]:
                errors.append(f"Sprint S{s} has reached the shared capacity")
    if errors:
        return jsonify({"error": "Validation failed", "details": errors}), 409

    # Enforce per-tribe TOTAL cap for this (tribe, resource) from temp.reserved_sprints
    reserved_row = fetch_one("""
        SELECT ta.reserved_sprints
        FROM temp_assignments ta
        WHERE ta.quarter_id = :qid
          AND ta.resource_id = :rid
          AND ta.tribe_name = :tname
        ORDER BY ta.id DESC
        LIMIT 1
        """, qid=qid, rid=rid, tname=tribe)
    cap_per_tribe = int((reserved_row or {}).get("reserved_sprints") or 0)
    if assign_type == "Dedicated":
        cap_per_tribe = max(cap_per_tribe, 6)

    already_cnt_row = fetch_one("""
        SELECT COALESCE(SUM(
          (CASE WHEN s1 THEN 1 ELSE 0 END) +
          (CASE WHEN s2 THEN 1 ELSE 0 END) +
          (CASE WHEN s3 THEN 1 ELSE 0 END) +
          (CASE WHEN s4 THEN 1 ELSE 0 END) +
          (CASE WHEN s5 THEN 1 ELSE 0 END) +
          (CASE WHEN s6 THEN 1 ELSE 0 END)
        ), 0) AS cnt
        FROM master_assignments
        WHERE quarter_id = :qid
          AND resource_name = :rname
          AND role = :rrole
          AND tribe_name = :tname
        """, qid=qid, rname=resource, rrole=role, tname=tribe)
    already_cnt = int((already_cnt_row or {}).get("cnt", 0))
    if already_cnt + len([s for s in selected_sprints if not tribe_has[s]]) > cap_per_tribe:
        return jsonify({"error": f"you’ve exceeded max allowed number of sprints ({cap_per_tribe})."}), 400

    # Build the six sprint booleans once
    svals_bool = {i: (i in selected_sprints) for i in range(1, 7)}

        # ONE statement: insert-or-update on (quarter, tribe, resource, role)
    sql = """
            INSERT INTO master_assignments (
            quarter_id, tribe_name, app_name, resource_name, role, assignment_type,
            s1, s2, s3, s4, s5, s6, edited, updated_at
        )
        VALUES (
            :qid, :tname, :appname, :rname, :rrole, :atype,
            :s1, :s2, :s3, :s4, :s5, :s6, FALSE, NOW()
        )
        ON CONFLICT (quarter_id, tribe_name, resource_name, role)
        DO UPDATE SET
            assignment_type = EXCLUDED.assignment_type,
            s1 = COALESCE(master_assignments.s1, FALSE) OR COALESCE(EXCLUDED.s1, FALSE),
            s2 = COALESCE(master_assignments.s2, FALSE) OR COALESCE(EXCLUDED.s2, FALSE),
            s3 = COALESCE(master_assignments.s3, FALSE) OR COALESCE(EXCLUDED.s3, FALSE),
            s4 = COALESCE(master_assignments.s4, FALSE) OR COALESCE(EXCLUDED.s4, FALSE),
            s5 = COALESCE(master_assignments.s5, FALSE) OR COALESCE(EXCLUDED.s5, FALSE),
            s6 = COALESCE(master_assignments.s6, FALSE) OR COALESCE(EXCLUDED.s6, FALSE),
            updated_at = NOW();
    """


    execute(sql, **{
        "qid": qid,
        "tname": tribe,
        "appname": temp["app_name"],
        "rname": resource,
        "rrole": role,
        "atype": assign_type,  # 'Shared' or 'Dedicated'
        "s1": svals_bool[1],
        "s2": svals_bool[2],
        "s3": svals_bool[3],
        "s4": svals_bool[4],
        "s5": svals_bool[5],
        "s6": svals_bool[6],
    })


    return jsonify({"ok": True})
