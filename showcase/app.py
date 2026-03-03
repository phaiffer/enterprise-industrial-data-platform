"""Web app for the portfolio-ready visualization layer."""

from __future__ import annotations

from pathlib import Path

from utils import get_dq_context, get_lineage_context, get_overview_context

BASE_DIR = Path(__file__).resolve().parent

try:
    from flask import Flask, render_template

    FRAMEWORK = "flask"
except ModuleNotFoundError:
    FRAMEWORK = "fastapi"

if FRAMEWORK == "flask":
    app = Flask(
        __name__,
        template_folder=str(BASE_DIR / "templates"),
        static_folder=str(BASE_DIR / "static"),
    )

    @app.route("/")
    def index():
        overview = get_overview_context()
        return render_template("index.html", overview=overview, active_page="overview")

    @app.route("/architecture")
    def architecture():
        return render_template("architecture.html", active_page="architecture")

    @app.route("/lineage")
    def lineage():
        lineage_data = get_lineage_context()
        return render_template("lineage.html", lineage=lineage_data, active_page="lineage")

    @app.route("/dq")
    def data_quality():
        dq_data = get_dq_context()
        return render_template("dq.html", dq=dq_data, active_page="dq")
else:
    try:
        import uvicorn
        from fastapi import FastAPI, Request
        from fastapi.responses import HTMLResponse
        from fastapi.staticfiles import StaticFiles
        from fastapi.templating import Jinja2Templates
    except ModuleNotFoundError as error:
        raise SystemExit(
            "Flask or FastAPI is required. Install one with "
            "`python -m pip install flask` or `python -m pip install fastapi uvicorn`."
        ) from error

    app = FastAPI(title="EIDP Showcase Visualization Layer")
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        overview = get_overview_context()
        context = {"request": request, "overview": overview, "active_page": "overview"}
        return templates.TemplateResponse("index.html", context)

    @app.get("/architecture", response_class=HTMLResponse)
    def architecture(request: Request):
        context = {"request": request, "active_page": "architecture"}
        return templates.TemplateResponse("architecture.html", context)

    @app.get("/lineage", response_class=HTMLResponse)
    def lineage(request: Request):
        lineage_data = get_lineage_context()
        context = {"request": request, "lineage": lineage_data, "active_page": "lineage"}
        return templates.TemplateResponse("lineage.html", context)

    @app.get("/dq", response_class=HTMLResponse)
    def data_quality(request: Request):
        dq_data = get_dq_context()
        context = {"request": request, "dq": dq_data, "active_page": "dq"}
        return templates.TemplateResponse("dq.html", context)


if __name__ == "__main__":
    if FRAMEWORK == "flask":
        app.run(host="0.0.0.0", port=8000, debug=False)
    else:
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
