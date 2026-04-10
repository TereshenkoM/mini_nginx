from pathlib import Path

import yaml

from src.domain.config import (
    AppConfig,
    LimitsConfig,
    LoggingConfig,
    MetricsConfig,
    TimeoutsConfig,
    UpstreamConfig,
)


def load_config(path: Path) -> AppConfig:
    data = yaml.safe_load(path.read_text()) or {}

    listen = data.get("listen")
    if not listen or ":" not in listen:
        raise ValueError("Config 'listen' must be in 'host:port' format")

    listen_host, listen_port_str = listen.rsplit(":", 1)
    listen_port = int(listen_port_str)

    upstreams_raw = data.get("upstreams") or []
    if not upstreams_raw:
        raise ValueError("Config must include at least one upstream")

    upstreams = [
        UpstreamConfig(host=entry["host"], port=int(entry["port"]))
        for entry in upstreams_raw
    ]

    timeouts_raw = data.get("timeouts") or {}
    limits_raw = data.get("limits") or {}
    logging_raw = data.get("logging") or {}
    metrics_raw = data.get("metrics") or {}

    timeouts = TimeoutsConfig(
        connect_ms=int(timeouts_raw.get("connect_ms", 0)),
        read_ms=int(timeouts_raw.get("read_ms", 0)),
        write_ms=int(timeouts_raw.get("write_ms", 0)),
        total_ms=int(timeouts_raw.get("total_ms", 0)),
    )
    limits = LimitsConfig(
        max_client_conns=int(limits_raw.get("max_client_conns", 0)),
        max_conns_per_upstream=int(limits_raw.get("max_conns_per_upstream", 0)),
        client_pool_size=int(limits_raw.get("client_pool_size", 0)),
    )
    logging = LoggingConfig(level=str(logging_raw.get("level", "info")))

    metrics_enabled = bool(metrics_raw.get("enabled", False))
    metrics_listen = str(metrics_raw.get("listen", "127.0.0.1:9090"))
    if ":" not in metrics_listen:
        raise ValueError("Config 'metrics.listen' must be in 'host:port' format")
    metrics_host, metrics_port_str = metrics_listen.rsplit(":", 1)
    metrics = MetricsConfig(
        enabled=metrics_enabled,
        listen_host=metrics_host,
        listen_port=int(metrics_port_str),
    )

    return AppConfig(
        listen_host=listen_host,
        listen_port=listen_port,
        upstreams=upstreams,
        timeouts=timeouts,
        limits=limits,
        logging=logging,
        metrics=metrics,
    )
