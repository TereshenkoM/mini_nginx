from pathlib import Path

from fastapi import FastAPI, Request

from src.infrastructure.config_loader import load_config

app = FastAPI()
config = load_config(Path("config.yaml"))
upstream = config.upstreams[0]


@app.get("/")
async def root(request: Request):
    host = request.headers.get("host", f"{upstream.host}:{upstream.port}")
    return {"hello": "world", "upstream": host}
