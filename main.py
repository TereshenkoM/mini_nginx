from pathlib import Path
import multiprocessing
import sys
import threading

from loguru import logger

from src.application.metrics import Metrics
from src.infrastructure.config_loader import load_config
from src.infrastructure.server.connection_handler import ConnectionHandler
from src.infrastructure.server.tcp_server import TCPServer
from src.interfaces.http.request_parser import HttpRequestParser
from src.interfaces.metrics_server import serve_metrics


def run_worker(config, enable_metrics: bool, reuse_port: bool) -> None:
    logger.remove()
    logger.add(sys.stderr, level=config.logging.level.upper())

    parser = HttpRequestParser()
    metrics = Metrics()
    connection_handler = ConnectionHandler(
        parser=parser,
        upstreams=config.upstreams,
        max_conns_per_upstream=config.limits.max_conns_per_upstream,
        timeouts=config.timeouts,
        metrics=metrics,
    )
    server_instance = TCPServer(
        connection_handler=connection_handler,
        max_client_conns=config.limits.max_client_conns,
        client_pool_size=config.limits.client_pool_size,
        metrics=metrics,
    )

    if enable_metrics:
        metrics_thread = threading.Thread(
            target=serve_metrics,
            args=(config.metrics.listen_host, config.metrics.listen_port, metrics),
            daemon=True,
        )
        metrics_thread.start()

    server_instance.serve_forever(
        host=config.listen_host,
        port=config.listen_port,
        reuse_port=reuse_port,
    )


if __name__ == "__main__":
    config = load_config(Path("config.yaml"))
    worker_processes = max(1, config.limits.worker_processes)
    if worker_processes == 1:
        run_worker(config, config.metrics.enabled, reuse_port=False)
    else:
        if config.metrics.enabled:
            logger.warning(
                "Metrics server is started only in worker 0 when worker_processes > 1"
            )
        processes: list[multiprocessing.Process] = []
        for index in range(worker_processes):
            enable_metrics = config.metrics.enabled and index == 0
            process = multiprocessing.Process(
                target=run_worker,
                args=(config, enable_metrics, True),
            )
            process.start()
            processes.append(process)
        for process in processes:
            process.join()
