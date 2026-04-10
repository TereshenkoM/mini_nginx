import asyncio
from asyncio import StreamReader, StreamWriter
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
            asyncio.Semaphore(max_client_conns) if max_client_conns > 0 else None
        )
        self._client_pool_size = client_pool_size
        self._queue: asyncio.Queue[tuple[StreamReader, StreamWriter]] = asyncio.Queue()
        self._metrics = metrics
        self._writers: set[StreamWriter] = set()
        self._workers: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        pool_size = self._client_pool_size if self._client_pool_size > 0 else 1
        for _ in range(pool_size):
            task = asyncio.create_task(self._worker())
            self._workers.add(task)
            task.add_done_callback(self._workers.discard)

    async def add_client(self, reader: StreamReader, writer: StreamWriter) -> None:
        if self._client_semaphore is not None:
            await self._client_semaphore.acquire()
        if self._metrics is not None:
            self._metrics.inc_active_connections()
        self._writers.add(writer)

        peer = writer.get_extra_info("peername")
        logger.info("Новое соединение: {}", peer)

        await self._queue.put((reader, writer))

    async def _worker(self) -> None:
        while True:
            reader, writer = await self._queue.get()
            try:
                await self._connect(reader, writer)
            finally:
                self._queue.task_done()

    async def _connect(self, reader: StreamReader, writer: StreamWriter) -> None:
        try:
            await self._connection_handler.handle(reader, writer)
        except asyncio.IncompleteReadError:
            logger.exception("Клиент оборвал соединение во время чтения")
            if self._metrics is not None:
                self._metrics.inc_errors()
        except (ConnectionError, OSError) as exc:
            logger.exception("Ошибка соединения: {}", exc)
            if self._metrics is not None:
                self._metrics.inc_errors()
        except Exception as exc:
            logger.exception("Необработанная ошибка: {}", exc)
            if self._metrics is not None:
                self._metrics.inc_errors()
        finally:
            if writer in self._writers:
                self._writers.remove(writer)

            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

            logger.info("Соединение закрыто")
            if self._metrics is not None:
                self._metrics.dec_active_connections()
            if self._client_semaphore is not None:
                self._client_semaphore.release()
