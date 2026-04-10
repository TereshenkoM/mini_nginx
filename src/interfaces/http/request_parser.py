from typing import final


class HttpParseError(Exception):
    pass


class HttpClientDisconnected(HttpParseError):
    pass


class BufferedSocketReader:
    def __init__(self, sock, max_line_bytes: int = 8192) -> None:
        self._sock = sock
        self._buffer = bytearray()
        self._max_line_bytes = max_line_bytes

    def readline(self) -> bytes:
        while True:
            index = self._buffer.find(b"\r\n")
            if index != -1:
                line = bytes(self._buffer[: index + 2])
                del self._buffer[: index + 2]
                return line
            if len(self._buffer) > self._max_line_bytes:
                raise HttpParseError("Line too long")
            data = self._sock.recv(4096)
            if not data:
                if self._buffer:
                    line = bytes(self._buffer)
                    self._buffer.clear()
                    return line
                return b""
            self._buffer.extend(data)

    def take_buffer(self) -> bytes:
        if not self._buffer:
            return b""
        data = bytes(self._buffer)
        self._buffer.clear()
        return data


@final
class HttpRequestParser:
    def parse(
        self, reader: BufferedSocketReader
    ) -> tuple[str, str, str, dict[str, str], bytes, bytes]:
        request_line = self._read_line(reader)
        if request_line == b"":
            raise HttpClientDisconnected("Empty request line")

        request_line_str = self._decode_line(request_line, "Request line is not valid UTF-8")
        parts = request_line_str.split(" ")
        if len(parts) != 3:
            raise HttpParseError("Invalid request line")

        headers_bytes = bytearray()
        headers: dict[str, str] = {}

        while True:
            line = self._read_line(reader)
            if line == b"":
                raise HttpClientDisconnected("Unexpected end of headers")
            if line == b"\r\n":
                break

            line_str = self._decode_line(line, "Header line is not valid UTF-8")
            if ":" not in line_str:
                raise HttpParseError("Invalid header line")

            name, value = line_str.split(":", 1)
            headers[name.strip()] = value.strip()
            headers_bytes.extend(line)

        raw_headers = request_line + headers_bytes + b"\r\n"
        body_prefix = reader.take_buffer()
        method, path, version = parts

        return method, path, version, headers, raw_headers, body_prefix

    @staticmethod
    def _read_line(reader: BufferedSocketReader) -> bytes:
        line = reader.readline()
        if line and not line.endswith(b"\r\n"):
            raise HttpParseError("Line does not end with CRLF")
        return line

    @staticmethod
    def _decode_line(line: bytes, error_message: str) -> str:
        try:
            return line.decode("utf-8").rstrip("\r\n")
        except UnicodeDecodeError as exc:
            raise HttpParseError(error_message) from exc
