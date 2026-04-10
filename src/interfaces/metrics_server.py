import socket
from contextlib import suppress

from loguru import logger

from src.application.metrics import Metrics


def handle_metrics_connection(client_sock: socket.socket, metrics: Metrics) -> None:
    try:
        request_line = client_sock.recv(4096)
        if not request_line:
            return
        try:
            request_line_str = request_line.decode("ascii", errors="ignore").strip()
        except UnicodeDecodeError:
            request_line_str = ""
        parts = request_line_str.split()
        path = parts[1] if len(parts) >= 2 else ""

        while True:
            line = client_sock.recv(4096)
            if not line or line in (b"\r\n", b"\n"):
                break

        if path != "/metrics":
            body = b"Not Found\n"
            response = (
                b"HTTP/1.1 404 Not Found\r\n"
                b"Content-Type: text/plain; charset=utf-8\r\n"
                + f"Content-Length: {len(body)}\r\n".encode("ascii")
                + b"Connection: close\r\n"
                + b"\r\n"
                + body
            )
            client_sock.sendall(response)
            return

        payload = metrics.render_prometheus().encode("utf-8")
        response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
            + f"Content-Length: {len(payload)}\r\n".encode("ascii")
            + b"Connection: close\r\n"
            + b"\r\n"
        )
        client_sock.sendall(response + payload)
    except Exception as exc:
        logger.exception("Metrics handler error: {}", exc)
    finally:
        with suppress(Exception):
            client_sock.close()


def serve_metrics(host: str, port: int, metrics: Metrics) -> None:
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((host, port))
    server_sock.listen()
    logger.info("Metrics server started on {}:{}", host, port)
    try:
        while True:
            client_sock, _ = server_sock.accept()
            handle_metrics_connection(client_sock, metrics)
    finally:
        server_sock.close()
