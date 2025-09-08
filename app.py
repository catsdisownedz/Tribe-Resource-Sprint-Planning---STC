# app.py
import os, time, webbrowser, sys, threading
from flask import Flask, render_template, g, request, Blueprint
from dotenv import load_dotenv
from time import perf_counter
from db import fetch_one

def _load_env_external():
    """Load .env from safe locations without bundling, and never override real OS env."""
    # If both are already set via OS env, do nothing.
    if os.getenv("DATABASE_URL") and os.getenv("FLASK_SECRET"):
        return

    from pathlib import Path
    candidates = []

    # When frozen: prefer a .env placed NEXT TO the executable
    if getattr(sys, "frozen", False):
        candidates.append(Path(sys.executable).parent / ".env")

    # Fallbacks for dev / non-frozen runs
    candidates.append(Path.cwd() / ".env")
    candidates.append(Path(__file__).resolve().parent / ".env")

    for p in candidates:
        if p and p.exists():
            load_dotenv(p, override=False)   # don't overwrite OS env
            break

# Load external .env if available
_load_env_external()

# --- Resolve base path for templates/static (works in dev and PyInstaller) ---
def _base_path():
    """
    Returns the folder where bundled assets (templates/static) live.
    - dev (python app.py): project directory
    - PyInstaller onefile: sys._MEIPASS (temp extraction dir)
    - PyInstaller onedir: directory next to the executable
    """
    if getattr(sys, "frozen", False):
        return getattr(sys, "_MEIPASS", os.path.dirname(sys.executable))
    return os.path.abspath(os.path.dirname(__file__))

def create_app():
    base = _base_path()
    template_dir = os.path.join(base, "templates")
    static_dir   = os.path.join(base, "static")

    app = Flask(
        __name__,
        template_folder=template_dir,
        static_folder=static_dir,
        static_url_path="/static",
    )
    app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET", "dev-secret")

    # --- non-blocking schema warm ---
    def _warm():
        with app.app_context():
            from routes.admin import _ensure_min_schema
            try:
                _ensure_min_schema()
            except Exception as e:
                app.logger.error("Schema warm failed: %s", e)

    # Kick it off right away in the background
    threading.Thread(target=_warm, daemon=True).start()

    @app.before_request
    def _t0():
        g._t0 = perf_counter()

    @app.after_request
    def _t1(resp):
        try:
            dt = (perf_counter() - g._t0) * 1000
            app.logger.info("HTTP %.1fms %s %s", dt, request.method, request.path)
        except Exception:
            pass
        return resp

    # Blueprints
    from routes.api import bp as api_bp
    from routes.admin import bp as admin_bp
    from routes.booking import bp as booking_bp
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.register_blueprint(booking_bp)

    @app.get("/")
    def index():
        # fetch current quarter name (null-safe)
        row = fetch_one("""
            SELECT COALESCE(name, code) AS title
            FROM quarters
            WHERE is_current = TRUE
            LIMIT 1
        """)
        current = (row or {}).get("title") or ""
        return render_template("index.html", current_quarter=current)

    return app


if __name__ == "__main__":
    app = create_app()
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5000"))
    url = f"http://{host}:{port}/"

    def _open():
        time.sleep(1.2)
        try:
            webbrowser.open_new(url)
        except Exception:
            pass

     # Detect frozen exe (PyInstaller)
    is_frozen = getattr(sys, "frozen", False)

    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        threading.Thread(target=_open, daemon=True).start()

    app.run(host=host, port=port, debug=(not is_frozen))