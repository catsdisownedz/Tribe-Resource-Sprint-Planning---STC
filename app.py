# app.py
import os, time, webbrowser
from flask import Flask, render_template, g, request
from dotenv import load_dotenv
from time import perf_counter

load_dotenv(override=True)
# app.py
import threading

def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="static")
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

    # blueprints (unchanged) ...

    # Blueprints
    from routes.api import bp as api_bp
    from routes.admin import bp as admin_bp
    from routes.booking import bp as booking_bp
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.register_blueprint(booking_bp)


    @app.get("/")
    def index():
        return render_template("index.html")

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

    import threading
    threading.Thread(target=_open, daemon=True).start()
    app.run(host=host, port=port, debug=True)
