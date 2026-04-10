import asyncio
import time
from asyncio import StreamReader, StreamWriter
from typing import final

from loguru import logger

from src.application.metrics import Metrics
from src.domain.config import TimeoutsConfig, UpstreamConfig
from src.interfaces.http.request_parser import HttpParseError, HttpRequestParser


@final
class ConnectionHandler:
    def __init__(
        self,
        parser: HttpRequestParser,
        upstreams: list[UpstreamConfig] | None = None,
        max_conns_per_upstream: int = 0,
        timeouts: TimeoutsConfig | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self._parser = parser
        self._upstreams = upstreams or [UpstreamConfig(host="127.0.0.1", port=8000)]
        self._upstream_index = 0
        self._upstream_lock = asyncio.Lock()
        self._upstream_semaphores = [
            asyncio.Semaphore(max_conns_per_upstream)
            for _ in self._upstreams
        ] if max_conns_per_upstream > 0 else None
        self._timeouts = timeouts
        self._metrics = metrics

    async def _next_upstream(self) -> tuple[int, UpstreamConfig]:
        index = self._upstream_index
        upstream = self._upstreams[index]
        self._upstream_index = (self._upstream_index + 1) % len(self._upstreams)
        return index, upstream

    async def _pipe(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        read_timeout: float | None,
        write_timeout: float | None,
        deadline: float | None,
        direction: str,
    ) -> None:
        while True:
            read_limit = self._remaining_timeout(read_timeout, deadline)
            data = await self._with_timeout(
                reader.read(64 * 1024),
                read_limit,
                f"{direction} read timeout",
            )
            if not data:
                break
            writer.write(data)
            write_limit = self._remaining_timeout(write_timeout, deadline)
            await self._with_timeout(
                writer.drain(),
                write_limit,
                f"{direction} write timeout",
            )

            logger.debug("Upstream pipe {} bytes {}", len(data), direction)

    @staticmethod
    def _remaining_timeout(op_timeout: float | None, deadline: float | None) -> float | None:
        if deadline is None and op_timeout is None:
            return None
        remaining = None
        if deadline is not None:
            remaining = max(0.0, deadline - time.monotonic())
        if op_timeout is None:
            return remaining
        if remaining is None:
            return op_timeout
        return min(op_timeout, remaining)

    @staticmethod
    async def _with_timeout(coro, timeout: float | None, label: str):
        try:
            return await asyncio.wait_for(coro, timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(label) from exc

    async def handle(self, reader: StreamReader, writer: StreamWriter) -> None:
        started_at = time.monotonic()
        error = False
        try:
            method, path, version, headers, raw_headers = await self._parser.parse(reader)
        except HttpParseError as exc:
            logger.warning("Bad request: {}", exc)
            error = True
            if self._metrics is not None:
                self._metrics.inc_errors()
            writer.close()
            await writer.wait_closed()
            return

        if self._metrics is not None:
            self._metrics.inc_requests()

        logger.info(
            "Request: method={} path={} version={}",
            method,
            path,
            version,
        )
        logger.info("Headers: {}", headers)

        timeouts = self._timeouts
        connect_timeout = None
        read_timeout = None
        write_timeout = None
        total_timeout = None
        if timeouts is not None:
            connect_timeout = timeouts.connect_ms / 1000 if timeouts.connect_ms > 0 else None
            read_timeout = timeouts.read_ms / 1000 if timeouts.read_ms > 0 else None
            write_timeout = timeouts.write_ms / 1000 if timeouts.write_ms > 0 else None
            total_timeout = timeouts.total_ms / 1000 if timeouts.total_ms > 0 else None

        deadline = None
        if total_timeout is not None:
            deadline = time.monotonic() + total_timeout

        upstream_index, upstream = await self._next_upstream()
        upstream_semaphore = None
        if self._upstream_semaphores is not None:
            upstream_semaphore = self._upstream_semaphores[upstream_index]
            await self._with_timeout(
                upstream_semaphore.acquire(),
                self._remaining_timeout(connect_timeout, deadline),
                "Upstream acquire timeout",
            )

        upstream_reader = None
        upstream_writer = None
        try:
            upstream_reader, upstream_writer = await self._with_timeout(
                asyncio.open_connection(upstream.host, upstream.port),
                self._remaining_timeout(connect_timeout, deadline),
                "Upstream connect timeout",
            )
            logger.info("Upstream connected {}:{}", upstream.host, upstream.port)
            upstream_writer.write(raw_headers)
            await self._with_timeout(
                upstream_writer.drain(),
                self._remaining_timeout(write_timeout, deadline),
                "Upstream write timeout",
            )

            client_to_upstream = asyncio.create_task(
                self._pipe(
                    reader,
                    upstream_writer,
                    read_timeout,
                    write_timeout,
                    deadline,
                    "client->upstream",
                )
            )
            upstream_to_client = asyncio.create_task(
                self._pipe(
                    upstream_reader,
                    writer,
                    read_timeout,
                    write_timeout,
                    deadline,
                    "upstream->client",
                )
            )

            done = set()
            try:
                done, _ = await self._with_timeout(
                    asyncio.wait(
                        {client_to_upstream, upstream_to_client},
                        return_when=asyncio.FIRST_COMPLETED,
                    ),
                    self._remaining_timeout(total_timeout, deadline),
                    "Total request timeout",
                )
            except TimeoutError as exc:
                logger.warning("Timeout: {}", exc)
                error = True
                if self._metrics is not None:
                    self._metrics.inc_errors()
                client_to_upstream.cancel()
                upstream_to_client.cancel()
                await asyncio.gather(client_to_upstream, upstream_to_client, return_exceptions=True)
                return

            if upstream_to_client in done:
                client_to_upstream.cancel()
                await asyncio.gather(client_to_upstream, return_exceptions=True)
            else:
                if not upstream_writer.is_closing():
                    try:
                        upstream_writer.write_eof()
                    except (AttributeError, OSError):
                        pass
                await upstream_to_client
        except Exception:
            error = True
            raise
        finally:
            if upstream_writer is not None:
                upstream_writer.close()
                await upstream_writer.wait_closed()
                logger.info("Upstream closed {}:{}", upstream.host, upstream.port)
            if upstream_semaphore is not None:
                upstream_semaphore.release()
            duration = time.monotonic() - started_at
            if self._metrics is not None:
                self._metrics.observe_request_duration(duration)
            if error:
                logger.info("Request completed with error in {:.3f}s", duration)
            else:
                logger.info("Request completed in {:.3f}s", duration)
