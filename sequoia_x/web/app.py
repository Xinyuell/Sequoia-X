"""FastAPI application factory for the local WebUI."""

from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError

from sequoia_x.core.config import Settings, get_settings
from sequoia_x.data.engine import DataEngine
from sequoia_x.web.api import create_api_router
from sequoia_x.web.jobs import InMemoryJobManager

STATIC_DIR = Path(__file__).parent / "static"


def create_app(
    settings: Settings | None = None,
    engine: DataEngine | None = None,
    jobs: InMemoryJobManager | None = None,
) -> FastAPI:
    load_dotenv()
    settings = settings or _load_web_settings()
    engine = engine or DataEngine(settings)
    jobs = jobs or InMemoryJobManager()

    app = FastAPI(title="Sequoia-X Local WebUI")
    app.state.settings = settings
    app.state.engine = engine
    app.state.jobs = jobs

    @app.middleware("http")
    async def no_cache_static_assets(request, call_next):
        response = await call_next(request)
        if request.url.path == "/" or request.url.path.startswith("/static/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    app.include_router(create_api_router())
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    return app


def _load_web_settings() -> Settings:
    try:
        return get_settings()
    except ValidationError:
        return Settings(feishu_webhook_url="https://example.com/sequoia-x-webui-disabled")
