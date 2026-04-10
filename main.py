import asyncio
from pathlib import Path
import sys

import uvloop
from loguru import logger

from src.application.metrics import Metrics
from src.infrastructure.config_loader import load_config
from src.infrastructure.server.connection_handler import ConnectionHandler
from src.infrastructure.server.tcp_server import TCPServer
from src.interfaces.http.request_parser import HttpRequestParser
from src.interfaces.metrics_server import handle_metrics


async def client_connected(reader, writer, server: TCPServer) -> None:
    await server.add_client(reader, writer)


async def main() -> None:
    config = load_config(Path("config.yaml"))
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

    server = await asyncio.start_server(
        lambda reader, writer: client_connected(reader, writer, server_instance),
        host=config.listen_host,
        port=config.listen_port,
    )
    await server_instance.start()

    sockets = server.sockets
    for sock in sockets:
        logger.info("Server started on {}", sock.getsockname())

    metrics_server = None
    if config.metrics.enabled:
        metrics_server = await asyncio.start_server(
            lambda reader, writer: handle_metrics(reader, writer, metrics),
            host=config.metrics.listen_host,
            port=config.metrics.listen_port,
        )
        for sock in metrics_server.sockets or []:
            logger.info("Metrics server started on {}", sock.getsockname())

    async with server:
        if metrics_server is not None:
            async with metrics_server:
                await asyncio.gather(
                    server.serve_forever(),
                    metrics_server.serve_forever(),
                )
        else:
            await server.serve_forever()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
