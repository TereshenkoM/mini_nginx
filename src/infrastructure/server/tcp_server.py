import queue
import socket
import threading
from contextlib import suppress
from typing import final

from loguru import logger

from src.application.metrics import Metrics
from src.infrastructure.server.connection_handler import ConnectionHandler


@final
class TCPServer:
    def __init__(
        self,
        connection_handler: ConnectionHandler,
        max_client_conns: int = 0,
        client_pool_size: int = 0,
        metrics: Metrics | None = None,
    ) -> None:
        self._connection_handler = connection_handler
        self._client_semaphore = (
            threading.Semaphore(max_client_conns) if max_client_conns > 0 else None
        )
        self._client_pool_size = client_pool_size if client_pool_size > 0 else 1
        self._queue: queue.Queue[tuple[socket.socket, tuple[str, int] | None]] = queue.Queue()
        self._metrics = metrics
        self._workers: list[threading.Thread] = []

    def start(self) -> None:
        for _ in range(self._client_pool_size):
            thread = threading.Thread(target=self._worker, daemon=True)
            thread.start()
            self._workers.append(thread)

    def serve_forever(self, host: str, port: int, reuse_port: bool = False) -> None:
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if reuse_port and hasattr(socket, "SO_REUSEPORT"):
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        server_sock.bind((host, port))
        server_sock.listen()
        logger.info("Server started on {}:{}", host, port)
        self.start()

        try:
            while True:
                client_sock, addr = server_sock.accept()
                if self._client_semaphore is not None:
                    self._client_semaphore.acquire()
                if self._metrics is not None:
                    self._metrics.inc_active_connections()
                logger.info("Новое соединение: {}", addr)
                self._queue.put((client_sock, addr))
        finally:
            server_sock.close()

    def _worker(self) -> None:
        while True:
            client_sock, _ = self._queue.get()
            try:
                self._connect(client_sock)
            finally:
                self._queue.task_done()

    def _connect(self, client_sock: socket.socket) -> None:
        try:
            self._connection_handler.handle(client_sock)
        except ConnectionError as exc:
            logger.exception("Ошибка соединения: {}", exc)
            if self._metrics is not None:
                self._metrics.inc_errors()
        except Exception as exc:
            logger.exception("Необработанная ошибка: {}", exc)
            if self._metrics is not None:
                self._metrics.inc_errors()
        finally:
            with suppress(Exception):
                client_sock.close()

            logger.info("Соединение закрыто")
            if self._metrics is not None:
                self._metrics.dec_active_connections()
            if self._client_semaphore is not None:
                self._client_semaphore.release()
