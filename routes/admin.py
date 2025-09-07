# routes/admin.py
# Admin dashboard: login, set current quarter, upload Excel, normalize + load into quarter & permanent tables.
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
import os, re, threading, secrets
import pandas as pd
from db import fetch_one, fetch_all, execute, get_current_qid

ADMIN_PW = (os.getenv('ADMIN_PASSWORD') or '').strip()
bp = Blueprint("admin", __name__, template_folder="../templates")
SCHEMA_WARMED = False

# ------------------------------
# In-memory progress registry for progressive uploads
# ------------------------------
_PROGRESS = {}  # job_id -> {"percent": int, "status": "running|done|error", "rows":int, "target_quarter_id":int, "error":str}


# ---------- schema helpers ----------
def _has_col(table: str, col: str) -> bool:
    row = fetch_one("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = :t AND column_name = :c
        LIMIT 1
    """, t=table, c=col)
    return bool(row)

def _has_table(table: str) -> bool:
    row = fetch_one("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = :t AND table_schema = 'public'
        LIMIT 1
    """, t=table)
    return bool(row)

def _col_type(table: str, col: str) -> str|None:
    row = fetch_one("""
        SELECT data_type
        FROM information_schema.columns
        WHERE table_name = :t AND column_name = :c
        LIMIT 1
    """, t=table, c=col)
    return (row or {}).get("data_type") if row else None

# Ensure minimal schema shape for admin flows
# Supports legacy → new shapes without breaking your existing db.
def _ensure_min_schema():
    # quarters: support "label" (initial) or "name" (UI expects name)
    if _has_col("quarters", "label") and not _has_col("quarters", "name"):
        execute("ALTER TABLE quarters RENAME COLUMN label TO name")
    if not _has_col("quarters", "name"):
        execute("ALTER TABLE quarters ADD COLUMN name text")
    if not _has_col("quarters", "is_current"):
        execute("ALTER TABLE quarters ADD COLUMN is_current boolean NOT NULL DEFAULT FALSE")
    if not _has_col("quarters", "created_at"):
        execute("ALTER TABLE quarters ADD COLUMN created_at timestamp NOT NULL DEFAULT NOW()")

    # resources table: make sure role column exists
    if not _has_col("resources", "role"):
        execute("ALTER TABLE resources ADD COLUMN role TEXT")

    # temp_assignments: ensure core columns exist
    if not _has_table("temp_assignments"):
        execute("""
        CREATE TABLE IF NOT EXISTS temp_assignments (
          id SERIAL PRIMARY KEY,
          quarter_id INT NOT NULL REFERENCES quarters(id) ON DELETE CASCADE,
          tribe_id INT,
          app_id INT,
          -- optional denormalized names (for history ease)
          tribe_name TEXT,
          app_name TEXT,
          -- resource linkage
          resource_id INT,
          resource_name TEXT,
          role TEXT,
          assign_type TEXT,
          reserved_sprints INT NOT NULL DEFAULT 0
            CHECK (reserved_sprints >= 0 AND reserved_sprints <= 6)
        )""")
        try:
            execute(
                "ALTER TABLE temp_assignments "
                "ADD CONSTRAINT temp_assignments_resource_id_fkey "
                "FOREIGN KEY (resource_id) REFERENCES resources(id) ON DELETE CASCADE"
            )
        except Exception:
            pass
    else:
        # add missing columns if needed
        if not _has_col("temp_assignments", "quarter_id"):
            execute("ALTER TABLE temp_assignments ADD COLUMN quarter_id INT NOT NULL REFERENCES quarters(id) ON DELETE CASCADE")
        if not _has_col("temp_assignments", "resource_id"):
            try:
                execute("ALTER TABLE temp_assignments ADD COLUMN resource_id INT")
                execute(
                    "ALTER TABLE temp_assignments "
                    "ADD CONSTRAINT temp_assignments_resource_id_fkey "
                    "FOREIGN KEY (resource_id) REFERENCES resources(id) ON DELETE CASCADE"
                )
            except Exception:
                pass

    # --- normalize legacy column names in temp_assignments ---
    if not _has_col("temp_assignments", "resource_name"):
        if _has_col("temp_assignments", "resource"):
            execute("ALTER TABLE temp_assignments RENAME COLUMN resource TO resource_name")
        else:
            execute("ALTER TABLE temp_assignments ADD COLUMN resource_name TEXT")
    if not _has_col("temp_assignments", "role"):
        execute("ALTER TABLE temp_assignments ADD COLUMN role TEXT")

    if not _has_col("temp_assignments", "assign_type"):
        execute("ALTER TABLE temp_assignments ADD COLUMN assign_type TEXT")
    try:
        execute("""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1
            FROM information_schema.table_constraints c
            WHERE c.table_name='temp_assignments'
              AND c.constraint_type='CHECK'
              AND c.constraint_name='temp_assignments_assign_type_chk'
          ) THEN
            ALTER TABLE temp_assignments
              ADD CONSTRAINT temp_assignments_assign_type_chk
              CHECK (assign_type IN ('Dedicated','Shared'));
          END IF;
        END $$;
        """)
    except Exception:
        pass

    if not _has_col("temp_assignments", "reserved_sprints"):
        execute("""
            ALTER TABLE temp_assignments
            ADD COLUMN reserved_sprints INT NOT NULL DEFAULT 0
        """)
        try:
            execute("""ALTER TABLE temp_assignments
                    ADD CONSTRAINT ta_reserved_chk
                    CHECK (reserved_sprints >= 0 AND reserved_sprints <= 6)""")
        except Exception:
            pass  # if check exists

    # history_temp_assignments shape (create if missing or add columns if present)
    if not _has_table("history_temp_assignments"):
        execute("""
            CREATE TABLE IF NOT EXISTS history_temp_assignments(
              quarter_id INT,
              orig_id INT,
              tribe_name TEXT,
              app_name TEXT,
              resource_id INT,
              resource_name TEXT,
              role TEXT,
              assign_type TEXT,
              reserved_sprints INT NOT NULL DEFAULT 0
                CHECK (reserved_sprints >= 0 AND reserved_sprints <= 6)
            )
        """)
    else:
        if not _has_col("history_temp_assignments", "orig_id"):
            execute("ALTER TABLE history_temp_assignments ADD COLUMN orig_id INT")
        if not _has_col("history_temp_assignments", "reserved_sprints"):
            execute("ALTER TABLE history_temp_assignments ADD COLUMN reserved_sprints INT NOT NULL DEFAULT 0")
            try:
                execute("""ALTER TABLE history_temp_assignments
                        ADD CONSTRAINT hta_reserved_chk
                        CHECK (reserved_sprints >= 0 AND reserved_sprints <= 6)""")
            except Exception:
                pass

    # Make resource_id nullable then ON DELETE SET NULL for history
    try:
        execute("""
        DO $$
        BEGIN
            IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'history_temp_assignments'
                AND column_name = 'resource_id'
                AND is_nullable = 'NO'
            ) THEN
            ALTER TABLE history_temp_assignments
                ALTER COLUMN resource_id DROP NOT NULL;
            END IF;
        END $$;
        """)
    except Exception:
        pass
    try:
        execute("""
        DO $$
        DECLARE
            conname text;
        BEGIN
            SELECT c.conname
            INTO conname
            FROM pg_constraint c
            JOIN pg_class t   ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE c.contype = 'f'
            AND t.relname = 'history_temp_assignments'
            AND n.nspname = 'public'
            AND c.confrelid = 'resources'::regclass;

            IF conname IS NOT NULL THEN
            EXECUTE format('ALTER TABLE history_temp_assignments DROP CONSTRAINT %I', conname);
            END IF;

            BEGIN
            ALTER TABLE history_temp_assignments
                ADD CONSTRAINT hta_resource_id_fkey
                FOREIGN KEY (resource_id) REFERENCES resources(id) ON DELETE SET NULL;
            EXCEPTION WHEN duplicate_object THEN
            END;
        END $$;
        """)
    except Exception:
        pass

    # ----- master_assignments -----
    if not _has_table("master_assignments"):
        execute("""
            CREATE TABLE IF NOT EXISTS master_assignments (
              id SERIAL PRIMARY KEY,
              quarter_id INT NOT NULL REFERENCES quarters(id) ON DELETE CASCADE,
              orig_id INT,
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
        if not _has_col("history_master_assignments", "orig_id"):
            execute("ALTER TABLE history_master_assignments ADD COLUMN orig_id INT")
        if not _has_col("master_assignments", "quarter_id"):
            execute("ALTER TABLE master_assignments ADD COLUMN quarter_id INT REFERENCES quarters(id) ON DELETE CASCADE")
        if not _has_col("master_assignments", "tribe_name"):
            execute("ALTER TABLE master_assignments ADD COLUMN tribe_name TEXT")
        if not _has_col("master_assignments", "app_name"):
            execute("ALTER TABLE master_assignments ADD COLUMN app_name TEXT")
        if not _has_col("master_assignments", "resource_name"):
            execute("ALTER TABLE master_assignments ADD COLUMN resource_name TEXT")
        if not _has_col("master_assignments", "role"):
            execute("ALTER TABLE master_assignments ADD COLUMN role TEXT")

        if not _has_col("master_assignments", "assignment_type") and _has_col("master_assignments", "assign_type"):
            execute("ALTER TABLE master_assignments RENAME COLUMN assign_type TO assignment_type")
        if not _has_col("master_assignments", "assignment_type"):
            execute("ALTER TABLE master_assignments ADD COLUMN assignment_type TEXT")

        for col in ("s1","s2","s3","s4","s5","s6"):
            if not _has_col("master_assignments", col):
                execute(f"ALTER TABLE master_assignments ADD COLUMN {col} BOOLEAN NOT NULL DEFAULT FALSE")
        if not _has_col("master_assignments", "edited"):
            execute("ALTER TABLE master_assignments ADD COLUMN edited BOOLEAN NOT NULL DEFAULT FALSE")
        if not _has_col("master_assignments", "updated_at"):
            execute("ALTER TABLE master_assignments ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT NOW()")

    try:
        execute("CREATE INDEX IF NOT EXISTS idx_master_assignments_qid ON master_assignments(quarter_id)")
    except Exception:
        pass


# ---------- utils ----------
def is_admin() -> bool:
    return bool(session.get("is_admin"))

def admin_required(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not is_admin():
            return redirect(url_for("admin.login", next=request.path))
        return fn(*args, **kwargs)
    return wrapper

def current_quarter_id():
    qid = get_current_qid()
    if not qid:
        raise RuntimeError("No current quarter set.")
    return qid

# Tiny progress helper
def _progress_update(job_id: str|None, percent: int|None=None, status: str|None=None, **extra):
    if not job_id:
        return
    rec = _PROGRESS.get(job_id) or {}
    if percent is not None:
        rec["percent"] = max(0, min(100, int(percent)))
    if status:
        rec["status"] = status
    for k, v in extra.items():
        rec[k] = v
    _PROGRESS[job_id] = rec


# =========================
# PAGES
# =========================
@bp.get("/login")
def login():
    return render_template("admin.html", page="login", error=None)

@bp.post("/login")
def do_login():
    pw = (request.form.get("password") or (request.json.get("password") if request.is_json else "") or "").strip()
    if not pw:
        return render_template("admin.html", page="login", error="Password is required"), 400
    if not ADMIN_PW:
        return render_template("admin.html", page="login", error="Server has no ADMIN_PASSWORD set"), 500
    if pw != ADMIN_PW:
        return render_template("admin.html", page="login", error="Wrong password"), 401
    session["is_admin"] = True
    return redirect(url_for("admin.dashboard"))

@bp.get("/logout")
def logout():
    session.pop("is_admin", None)
    return redirect(url_for("index"))

@bp.get("/")
@admin_required
def dashboard():
    # Build a COALESCE() only from columns that exist
    title_cols = []
    if _has_col("quarters", "code"):
        title_cols.append("code")
    if _has_col("quarters", "name"):
        title_cols.append("name")
    if _has_col("quarters", "label"):
        title_cols.append("label")
    title_expr = "COALESCE(" + ", ".join(title_cols) + ")" if title_cols else "'(no title)'"

    quarters = fetch_all(f"""
      SELECT id, {title_expr} AS name, is_current, created_at
      FROM quarters
      ORDER BY created_at DESC
    """)
    current = next((q for q in quarters if q.get("is_current")), None)
    return render_template("admin.html", page="dashboard", quarters=quarters, current=current)


# =========================
# CURRENT QUARTER APIs
# (mounted under this blueprint; URL will be /admin/api/current-quarter)
# =========================
@bp.get("/api/current-quarter")
def api_current_quarter():
    qid = get_current_qid()
    if not qid:
        return jsonify({"name": ""})
    row = fetch_one("SELECT name FROM quarters WHERE id = :id", id=qid)
    return jsonify({"name": (row or {}).get("name") or ""})


@bp.post("/set-quarter")
@admin_required
def set_quarter():
    _ensure_min_schema()
    qname = (request.form.get("quarter_name")
             or (request.json.get("quarter_name") if request.is_json else "")
             or "").strip()
    if not qname:
        return jsonify({"error": "quarter name required"}), 400

    # Detect which title column the table actually has (code > name > label).
    if _has_col("quarters", "code"):
        qcol = "code"
    elif _has_col("quarters", "name"):
        qcol = "name"
    elif _has_col("quarters", "label"):
        qcol = "label"
    else:
        execute("ALTER TABLE quarters ADD COLUMN code TEXT")
        qcol = "code"

    row = fetch_one(f"SELECT id FROM quarters WHERE {qcol} = :v LIMIT 1", v=qname)
    if not row:
        execute(f"INSERT INTO quarters({qcol}, is_current, created_at) VALUES (:v, TRUE, NOW())", v=qname)
        row = fetch_one(f"SELECT id FROM quarters WHERE {qcol} = :v LIMIT 1", v=qname)
    else:
        execute("UPDATE quarters SET is_current = TRUE WHERE id = :id", id=row["id"])

    qid = int(row["id"])
    execute("UPDATE quarters SET is_current = FALSE WHERE id <> :id", id=qid)
    return jsonify({"ok": True, "quarter_id": qid, "quarter_name": qname})


# =========================
# VALIDATE (unchanged)
# =========================
@bp.post("/upload-validate")
def upload_validate():
    file = request.files.get("file")
    if not file:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400

    name = (file.filename or "").lower()
    if name.endswith((".xlsx", ".xls")):
        df = pd.read_excel(file)
    else:
        df = pd.read_csv(file)

    cols = [c.strip().lower() for c in df.columns]
    rename = {
        "tribe": "tribe",
        "app": "app",
        "role": "role",
        "reserved sprints": "reserved_sprints",
        "reserved_sprints": "reserved_sprints",
        "resource": "resource",
    }
    colmap = {}
    for c in rename:
        if c in cols:
            colmap[df.columns[cols.index(c)]] = rename[c]
    missing = [k for k in ("tribe","app","role","reserved_sprints","resource") if k not in colmap.values()]
    if missing:
        return jsonify({"ok": False, "error": f"Missing columns: {missing}"}), 400

    df = df.rename(columns=colmap)[["tribe","app","role","reserved_sprints","resource"]]

    df["tribe"] = df["tribe"].astype(str).str.strip()
    df["app"] = df["app"].astype(str).str.strip()
    df["role"] = df["role"].astype(str).str.strip()
    df["resource"] = df["resource"].astype(str).str.strip()
    df["reserved_sprints"] = pd.to_numeric(df["reserved_sprints"], errors="coerce").fillna(0).astype(int)

    conflicts = []
    for res_name, grp in df.groupby("resource", dropna=False):
        total = int(grp["reserved_sprints"].sum())
        if total > 6:
            row_numbers = (grp.index + 2).tolist()
            preview = grp[["tribe","app","role","reserved_sprints","resource"]].to_dict(orient="records")
            conflicts.append({
                "resource": res_name,
                "total_reserved": total,
                "rows": row_numbers,
                "rows_preview": preview
            })

    return jsonify({"ok": len(conflicts) == 0, "conflicts": conflicts})


# =========================
# CORE UPLOAD IMPLEMENTATION
# (used by both /upload and progressive /upload_excel_progress)
# =========================
def _normalize_and_classify(df: pd.DataFrame) -> pd.DataFrame:
    # normalize expected columns
    df.columns = [str(c).strip().lower() for c in df.columns]
    rename_in = {
        "reserved sprints": "reserved_sprints",
        "assignment type": "assign_type",
        "assign_type": "assign_type"
    }
    for k, v in rename_in.items():
        if k in df.columns and v not in df.columns:
            df.rename(columns={k: v}, inplace=True)

    required_cols = {"tribe","app","resource","role","reserved_sprints"}
    missing = [c for c in required_cols if c not in set(df.columns)]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    def _canon(s: str) -> str:
        s = str(s or "").strip()
        s = re.sub(r"\s+", " ", s)
        return s
    def _title(s: str) -> str:
        s = _canon(s)
        return s[:1].upper() + s[1:] if s else s

    df["tribe"]    = df["tribe"].map(_canon)
    df["tribe"]    = df["tribe"].str.replace(r"\bops\b", "Operations", regex=True)
    df["app"]      = df["app"].map(_canon)
    df["resource"] = df["resource"].map(_title)
    df["role"]     = df["role"].map(_title)

    df["reserved_sprints"] = pd.to_numeric(df["reserved_sprints"], errors="coerce").fillna(0).astype(int)
    df.loc[df["reserved_sprints"] < 0, "reserved_sprints"] = 0
    df.loc[df["reserved_sprints"] > 6, "reserved_sprints"] = 6
    df = df.drop_duplicates(subset=["tribe","app","resource","role"])

    agg = df.groupby("resource").agg(
        total_reserved=("reserved_sprints","sum"),
        tribes=("tribe","nunique"),
    ).reset_index()

    atype_map = {}
    for row in agg.itertuples(index=False):
        if row.tribes == 1 and row.total_reserved == 6:
            atype = "Dedicated"
        else:
            atype = "Shared"
        atype_map[row.resource] = atype
    df["assign_type"] = df["resource"].map(atype_map)
    return df


def _perform_upload(df: pd.DataFrame, target: str, new_qname: str|None, progress=None) -> tuple[int,int]:
    """
    Returns (rows_inserted, target_quarter_id).
    `progress(pct)` can be passed to update progress 1..100.
    """
    _ensure_min_schema()
    if progress: progress(1)

    df = _normalize_and_classify(df)
    rows_total = int(len(df))

    cur = fetch_one("SELECT id FROM quarters WHERE is_current = TRUE LIMIT 1")
    if not cur and target == "current":
        raise RuntimeError("No current quarter is set. Please set it first.")

    # Resolve target qid
    if target == "new":
        if not new_qname:
            raise RuntimeError("Please enter the new quarter name before uploading.")

        if _has_col("quarters", "code"):
            qcol = "code"
        elif _has_col("quarters", "name"):
            qcol = "name"
        elif _has_col("quarters", "label"):
            qcol = "label"
        else:
            execute("ALTER TABLE quarters ADD COLUMN code TEXT")
            qcol = "code"

        row = fetch_one(f"SELECT id FROM quarters WHERE {qcol} = :name", name=new_qname)
        if not row:
            execute(f"INSERT INTO quarters({qcol}, is_current, created_at) VALUES (:name, FALSE, NOW())", name=new_qname)
            row = fetch_one(f"SELECT id FROM quarters WHERE {qcol} = :name", name=new_qname)

        qid_target = row["id"]
        qid_snapshot = cur["id"] if cur else None
    else:
        qid_target = cur["id"]
        qid_snapshot = cur["id"]

    # Simple progress plan: schema/snapshot(0-20), reseed dims(20-50), inserts(50-95), finalize(95-100)
    if progress: progress(10)

    execute("BEGIN")
    try:
        # --- SNAPSHOT (if any) ---
        if qid_snapshot is not None:
            if _has_col("master_assignments", "assignment_type"):
                ma_type_expr = "assignment_type"
            elif _has_col("master_assignments", "assign_type"):
                ma_type_expr = "assign_type"
            else:
                ma_type_expr = "NULL::text"

            def _s(col: str) -> str:
                dt = _col_type("master_assignments", col)
                return col if (dt and dt.lower() == "boolean") else f"({col} <> 0)"

            if not _has_table("history_resources"):
                execute("""
                    CREATE TABLE IF NOT EXISTS history_resources(
                      quarter_id INT NOT NULL,
                      id INT,
                      name TEXT,
                      role TEXT
                    )""")
            if not _has_table("history_tribes"):
                execute("""
                    CREATE TABLE IF NOT EXISTS history_tribes(
                      quarter_id INT NOT NULL,
                      id INT,
                      name TEXT
                    )""")
            if not _has_table("history_apps"):
                execute("""
                    CREATE TABLE IF NOT EXISTS history_apps(
                      quarter_id INT NOT NULL,
                      id INT,
                      name TEXT
                    )""")
            if not _has_table("history_temp_assignments"):
                execute("""
                    CREATE TABLE IF NOT EXISTS history_temp_assignments(
                      quarter_id INT,
                      id INT,
                      tribe_name TEXT,
                      app_name TEXT,
                      resource_id INT,
                      resource_name TEXT,
                      role TEXT,
                      assign_type TEXT,
                      reserved_sprints INT NOT NULL DEFAULT 0
                    )""")
            if not _has_table("history_master_assignments"):
                execute("""
                    CREATE TABLE IF NOT EXISTS history_master_assignments(
                      quarter_id INT,
                      id INT,
                      tribe_name TEXT,
                      app_name TEXT,
                      resource_name TEXT,
                      role TEXT,
                      assignment_type TEXT,
                      s1 BOOLEAN, s2 BOOLEAN, s3 BOOLEAN,
                      s4 BOOLEAN, s5 BOOLEAN, s6 BOOLEAN,
                      edited BOOLEAN, updated_at TIMESTAMP
                    )""")
            if not _has_col("history_temp_assignments", "reserved_sprints"):
                execute("ALTER TABLE history_temp_assignments ADD COLUMN reserved_sprints INT NOT NULL DEFAULT 0")

            execute("DELETE FROM history_resources WHERE quarter_id = :qid", qid=qid_snapshot)
            execute("DELETE FROM history_tribes WHERE quarter_id = :qid", qid=qid_snapshot)
            execute("DELETE FROM history_apps WHERE quarter_id = :qid", qid=qid_snapshot)
            execute("DELETE FROM history_temp_assignments WHERE quarter_id = :qid", qid=qid_snapshot)
            execute("DELETE FROM history_master_assignments WHERE quarter_id = :qid", qid=qid_snapshot)

            execute("INSERT INTO history_resources(quarter_id,id,name,role) SELECT :qid,id,name,role FROM resources", qid=qid_snapshot)
            execute("INSERT INTO history_tribes(quarter_id,id,name) SELECT :qid,id,name FROM tribes", qid=qid_snapshot)
            execute("INSERT INTO history_apps(quarter_id,id,name) SELECT :qid,id,name FROM apps", qid=qid_snapshot)

            execute(f"""
              INSERT INTO history_master_assignments(
                quarter_id, orig_id, tribe_name, app_name, resource_name, role, assignment_type,
                s1,s2,s3,s4,s5,s6, edited, updated_at
              )
              SELECT :qid, id, tribe_name, app_name, resource_name, role, {ma_type_expr},
                     {_s('s1')}::boolean AS s1,
                     {_s('s2')}::boolean AS s2,
                     {_s('s3')}::boolean AS s3,
                     {_s('s4')}::boolean AS s4,
                     {_s('s5')}::boolean AS s5,
                     {_s('s6')}::boolean AS s6,
                     edited, updated_at
              FROM master_assignments
            """, qid=qid_snapshot)

            execute("""
              INSERT INTO history_temp_assignments(
                quarter_id, orig_id, tribe_name, app_name, resource_id, resource_name, role, assign_type, reserved_sprints
              )
              SELECT :qid,
                     ta.id as orig_id,
                     COALESCE(tr.name, ta.tribe_name) AS tribe_name,
                     COALESCE(ap.name, ta.app_name)   AS app_name,
                     ta.resource_id, ta.resource_name, ta.role, ta.assign_type, ta.reserved_sprints
              FROM temp_assignments ta
              LEFT JOIN tribes tr ON tr.id = ta.tribe_id
              LEFT JOIN apps   ap ON ap.id = ta.app_id
            """, qid=qid_snapshot)

        if progress: progress(20)

        # --- RESET working sets (FK-safe) ---
        execute("""
        DELETE FROM master_assignments;
        DELETE FROM temp_assignments;
        DELETE FROM resources;
        DELETE FROM tribes;
        DELETE FROM apps;
        """)

        # Reset sequences like RESTART IDENTITY
        execute("""
        DO $$
        BEGIN
        IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'master_assignments') THEN
            PERFORM setval(pg_get_serial_sequence('master_assignments','id'), 1, false);
        END IF;
        IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'temp_assignments') THEN
            PERFORM setval(pg_get_serial_sequence('temp_assignments','id'), 1, false);
        END IF;
        IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'resources') THEN
            PERFORM setval(pg_get_serial_sequence('resources','id'), 1, false);
        END IF;
        IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'tribes') THEN
            PERFORM setval(pg_get_serial_sequence('tribes','id'), 1, false);
        END IF;
        IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'apps') THEN
            PERFORM setval(pg_get_serial_sequence('apps','id'), 1, false);
        END IF;
        END $$;
        """)

        # --- RESEED dimensions ---
        tribes_u = sorted(df["tribe"].unique())
        apps_u   = sorted(df["app"].unique())
        res_u    = df[["resource","role"]].drop_duplicates(subset=["resource"])

        total_ops = len(tribes_u) + len(apps_u) + len(res_u) + rows_total
        done_ops  = 0
        def _bump():
            nonlocal done_ops
            done_ops += 1
            pct = 20 + int((done_ops / max(1,total_ops)) * 75)  # up to 95%
            if progress: progress(min(95, pct))

        for t in tribes_u:
            execute("INSERT INTO tribes(name) VALUES (:n) ON CONFLICT (name) DO NOTHING", n=t)
            _bump()
        for a in apps_u:
            execute("INSERT INTO apps(name) VALUES (:n) ON CONFLICT (name) DO NOTHING", n=a)
            _bump()
        for r_name, r_role in res_u.itertuples(index=False):
            execute("""
                INSERT INTO resources(name, role) VALUES (:n,:role)
                ON CONFLICT (name) DO UPDATE SET role = EXCLUDED.role
            """, n=r_name, role=r_role)
            _bump()

        # --- Insert temp_assignments ---
        ta_has_tribe_id   = _has_col("temp_assignments", "tribe_id")
        ta_has_app_id     = _has_col("temp_assignments", "app_id")
        ta_has_tribe_name = _has_col("temp_assignments", "tribe_name")
        ta_has_app_name   = _has_col("temp_assignments", "app_name")
        ta_has_res_id     = _has_col("temp_assignments", "resource_id")

        if not ta_has_tribe_id and not ta_has_tribe_name:
            execute("ALTER TABLE temp_assignments ADD COLUMN tribe_name TEXT")
            ta_has_tribe_name = True
        if not ta_has_app_id and not ta_has_app_name:
            execute("ALTER TABLE temp_assignments ADD COLUMN app_name TEXT")
            ta_has_app_name = True
        if not ta_has_res_id:
            execute("ALTER TABLE temp_assignments ADD COLUMN resource_id INT")
            ta_has_res_id = True

        for row in df.itertuples(index=False):
            params = {
                "qid": qid_target,
                "tribe": row.tribe, "app": row.app,
                "rname": row.resource, "role": row.role,
                "atype": row.assign_type,
                "rs": int(row.reserved_sprints)
            }
            cols = ["quarter_id"]
            vals = [":qid"]
            if ta_has_tribe_id:
                cols += ["tribe_id"]; vals += ["(SELECT id FROM tribes WHERE name=:tribe)"]
            if ta_has_app_id:
                cols += ["app_id"]; vals += ["(SELECT id FROM apps WHERE name=:app)"]
            if ta_has_tribe_name:
                cols += ["tribe_name"]; vals += [":tribe"]
            if ta_has_app_name:
                cols += ["app_name"]; vals += [":app"]
            if ta_has_res_id:
                cols += ["resource_id"]; vals += ["(SELECT id FROM resources WHERE name=:rname)"]
            cols += ["resource_name", "role", "assign_type", "reserved_sprints"]
            vals += [":rname", ":role", ":atype", ":rs"]
            sql = f"INSERT INTO temp_assignments({', '.join(cols)}) VALUES ({', '.join(vals)})"
            execute(sql, **params)
            _bump()

        execute("COMMIT")

        # If user asked to create a new quarter, make it current after upload finishes
        if target == "new":
            execute("UPDATE quarters SET is_current = FALSE")
            execute("UPDATE quarters SET is_current = TRUE WHERE id = :id", id=qid_target)

        if progress: progress(100)
        return rows_total, qid_target

    except Exception:
        execute("ROLLBACK")
        raise


# =========================
# ORIGINAL UPLOAD (kept for compatibility)
# =========================
@bp.post("/upload")
@admin_required
def upload_excel():
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "Missing file"}), 400

    try:
        df = pd.read_excel(file)
    except Exception as e:
        return jsonify({"error": f"Failed to read Excel: {e}"}), 400

    target = (request.form.get("target")
              or (request.json.get("target") if request.is_json else "")
              or "current").strip().lower()
    new_qname = (request.form.get("new_quarter_name")
                 or (request.json.get("new_quarter_name") if request.is_json else "")
                 or "").strip()

    try:
        rows, qid = _perform_upload(df, target, new_qname, progress=None)
        return jsonify({"ok": True, "rows": int(rows), "target_quarter_id": int(qid)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# =========================
# PROGRESSIVE UPLOAD (new)
# =========================
@bp.post("/upload_excel_progress")
@admin_required
def upload_excel_progress():
    """
    Same form-data as /upload. Returns {"job_id": "..."} immediately and processes in background.
    Client should poll /admin/upload_progress/<job_id>.
    """
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "Missing file"}), 400

    target = (request.form.get("target")
              or (request.json.get("target") if request.is_json else "")
              or "current").strip().lower()
    new_qname = (request.form.get("new_quarter_name")
                 or (request.json.get("new_quarter_name") if request.is_json else "")
                 or "").strip()

    try:
        df = pd.read_excel(f)
    except Exception as e:
        return jsonify({"error": f"Failed to read Excel: {e}"}), 400

    job_id = secrets.token_hex(8)
    _PROGRESS[job_id] = {"percent": 1, "status": "running"}

    def _worker():
        try:
            def cb(p):
                _progress_update(job_id, percent=p, status="running")
            rows, qid = _perform_upload(df, target, new_qname, progress=cb)
            _progress_update(job_id, percent=100, status="done", rows=int(rows), target_quarter_id=int(qid))
        except Exception as e:
            _progress_update(job_id, percent=100, status="error", error=str(e))

    threading.Thread(target=_worker, daemon=True).start()
    return jsonify({"job_id": job_id}), 202


@bp.get("/upload_progress/<job_id>")
@admin_required
def upload_progress(job_id):
    p = _PROGRESS.get(job_id)
    if not p:
        return jsonify({"percent": 0, "status": "unknown"}), 404
    return jsonify({
        "percent": p.get("percent", 0),
        "status": p.get("status", "running"),
        "rows": p.get("rows"),
        "target_quarter_id": p.get("target_quarter_id"),
        "error": p.get("error")
    })
