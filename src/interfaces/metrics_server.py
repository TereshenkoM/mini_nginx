from asyncio import StreamReader, StreamWriter

from loguru import logger

from src.application.metrics import Metrics


async def handle_metrics(reader: StreamReader, writer: StreamWriter, metrics: Metrics) -> None:
    try:
        request_line = await reader.readline()
        if not request_line:
            writer.close()
            await writer.wait_closed()
            return
        try:
            request_line_str = request_line.decode("ascii").strip()
        except UnicodeDecodeError:
            request_line_str = ""
        parts = request_line_str.split()
        path = parts[1] if len(parts) >= 2 else ""

        while True:
            line = await reader.readline()
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
            writer.write(response)
            await writer.drain()
            return

        payload = metrics.render_prometheus().encode("utf-8")
        response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
            + f"Content-Length: {len(payload)}\r\n".encode("ascii")
            + b"Connection: close\r\n"
            + b"\r\n"
        )
        writer.write(response + payload)
        await writer.drain()
    except Exception as exc:
        logger.exception("Metrics handler error: {}", exc)
    finally:
        writer.close()
        await writer.wait_closed()
