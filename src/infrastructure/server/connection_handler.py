import socket
import threading
import time
from contextlib import suppress
from typing import final

from loguru import logger

from src.application.metrics import Metrics
from src.domain.config import TimeoutsConfig, UpstreamConfig
from src.interfaces.http.request_parser import (
    BufferedSocketReader,
    HttpClientDisconnected,
    HttpParseError,
    HttpRequestParser,
)


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
        self._upstream_lock = threading.Lock()
        self._upstream_semaphores = (
            [threading.Semaphore(max_conns_per_upstream) for _ in self._upstreams]
            if max_conns_per_upstream > 0
            else None
        )
        self._timeouts = timeouts
        self._metrics = metrics

    def _next_upstream(self) -> tuple[int, UpstreamConfig]:
        with self._upstream_lock:
            index = self._upstream_index
            upstream = self._upstreams[index]
            self._upstream_index = (self._upstream_index + 1) % len(self._upstreams)
        return index, upstream

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
    def _min_timeout(*values: float | None) -> float | None:
        candidates = [value for value in values if value is not None]
        return min(candidates) if candidates else None

    @staticmethod
    def _pipe(
        src: socket.socket,
        dst: socket.socket,
        stop_event: threading.Event,
        direction: str,
        deadline: float | None,
    ) -> None:
        while not stop_event.is_set():
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"{direction} total timeout")
            try:
                data = src.recv(64 * 1024)
            except socket.timeout as exc:
                if stop_event.is_set():
                    break
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError(f"{direction} read timeout") from exc
                continue
            if not data:
                stop_event.set()
                break
            try:
                dst.sendall(data)
            except socket.timeout as exc:
                raise TimeoutError(f"{direction} write timeout") from exc

            logger.debug("Upstream pipe {} bytes {}", len(data), direction)

    @staticmethod
    def _thread_pipe(
        src: socket.socket,
        dst: socket.socket,
        stop_event: threading.Event,
        errors: list[Exception],
        direction: str,
        deadline: float | None,
    ) -> None:
        try:
            ConnectionHandler._pipe(src, dst, stop_event, direction, deadline)
        except Exception as exc:
            errors.append(exc)
            stop_event.set()

    def handle(self, client_sock: socket.socket) -> None:
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

        socket_timeout = self._min_timeout(read_timeout, write_timeout)
        if socket_timeout is None:
            socket_timeout = 1.0
        client_sock.settimeout(socket_timeout)

        reader = BufferedSocketReader(client_sock)

        while True:
            started_at = time.monotonic()
            error = False
            deadline = None
            if total_timeout is not None:
                deadline = time.monotonic() + total_timeout

            try:
                method, path, version, headers, raw_headers, body_prefix = self._parser.parse(
                    reader
                )
            except socket.timeout as exc:
                logger.warning("Timeout: {}", exc)
                error = True
                if self._metrics is not None:
                    self._metrics.inc_errors()
                return
            except HttpClientDisconnected:
                return
            except HttpParseError as exc:
                logger.warning("Bad request: {}", exc)
                error = True
                if self._metrics is not None:
                    self._metrics.inc_errors()
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

            headers_ci = {name.lower(): value for name, value in headers.items()}
            connection_header = headers_ci.get("connection", "").lower()
            close_after_response = (
                connection_header == "close"
                or (version == "HTTP/1.0" and connection_header != "keep-alive")
            )

            upstream_index, upstream = self._next_upstream()
            upstream_semaphore = None
            if self._upstream_semaphores is not None:
                upstream_semaphore = self._upstream_semaphores[upstream_index]
                timeout = self._remaining_timeout(connect_timeout, deadline)
                if timeout is None:
                    upstream_semaphore.acquire()
                elif not upstream_semaphore.acquire(timeout=timeout):
                    error = True
                    if self._metrics is not None:
                        self._metrics.inc_errors()
                    raise TimeoutError("Upstream acquire timeout")

            upstream_sock = None
            try:
                timeout = self._remaining_timeout(connect_timeout, deadline)
                upstream_sock = socket.create_connection(
                    (upstream.host, upstream.port), timeout=timeout
                )
                upstream_sock.settimeout(socket_timeout)
                logger.info("Upstream connected {}:{}", upstream.host, upstream.port)
                upstream_sock.sendall(raw_headers)
                if body_prefix:
                    upstream_sock.sendall(body_prefix)

                stop_event = threading.Event()
                errors: list[Exception] = []
                client_to_upstream = threading.Thread(
                    target=self._thread_pipe,
                    args=(
                        client_sock,
                        upstream_sock,
                        stop_event,
                        errors,
                        "client->upstream",
                        deadline,
                    ),
                    daemon=True,
                )
                upstream_to_client = threading.Thread(
                    target=self._thread_pipe,
                    args=(
                        upstream_sock,
                        client_sock,
                        stop_event,
                        errors,
                        "upstream->client",
                        deadline,
                    ),
                    daemon=True,
                )

                client_to_upstream.start()
                upstream_to_client.start()

                if total_timeout is None:
                    stop_event.wait()
                else:
                    remaining = self._remaining_timeout(total_timeout, deadline)
                    if remaining is not None and not stop_event.wait(timeout=remaining):
                        errors.append(TimeoutError("Total request timeout"))
                        stop_event.set()

                with suppress(OSError):
                    upstream_sock.shutdown(socket.SHUT_RDWR)
                if close_after_response:
                    with suppress(OSError):
                        client_sock.shutdown(socket.SHUT_RDWR)

                client_to_upstream.join()
                upstream_to_client.join()

                if errors:
                    error = True
                    for exc in errors:
                        logger.warning("Timeout: {}", exc)
                    if self._metrics is not None:
                        self._metrics.inc_errors()
                    return
            except Exception:
                error = True
                raise
            finally:
                if upstream_sock is not None:
                    try:
                        upstream_sock.close()
                    except OSError:
                        pass
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

            if close_after_response:
                return
