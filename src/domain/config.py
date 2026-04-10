from dataclasses import dataclass


@dataclass(slots=True)
class UpstreamConfig:
    host: str
    port: int


@dataclass(slots=True)
class TimeoutsConfig:
    connect_ms: int
    read_ms: int
    write_ms: int
    total_ms: int


@dataclass(slots=True)
class LimitsConfig:
    max_client_conns: int
    max_conns_per_upstream: int
    client_pool_size: int


@dataclass(slots=True)
class LoggingConfig:
    level: str


@dataclass(slots=True)
class MetricsConfig:
    enabled: bool
    listen_host: str
    listen_port: int


@dataclass(slots=True)
class AppConfig:
    listen_host: str
    listen_port: int
    upstreams: list[UpstreamConfig]
    timeouts: TimeoutsConfig
    limits: LimitsConfig
    logging: LoggingConfig
    metrics: MetricsConfig
