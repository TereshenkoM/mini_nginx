from asyncio import StreamReader
from typing import final


class HttpParseError(Exception):
    pass


class HttpClientDisconnected(HttpParseError):
    pass


@final
class HttpRequestParser:
    async def parse(self, reader: StreamReader) -> tuple[str, str, str, dict[str, str], bytes]:
        request_line = await self._read_line(reader)
        if request_line == b"":
            raise HttpClientDisconnected("Empty request line")

        request_line_str = self._decode_line(request_line, "Request line is not valid UTF-8")
        parts = request_line_str.split(" ")
        if len(parts) != 3:
            raise HttpParseError("Invalid request line")

        headers_bytes = bytearray()
        headers: dict[str, str] = {}

        while True:
            line = await self._read_line(reader)
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
        method, path, version = parts

        return method, path, version, headers, raw_headers

    @staticmethod
    async def _read_line(reader: StreamReader) -> bytes:
        line = await reader.readline()
        if line and not line.endswith(b"\r\n"):
            raise HttpParseError("Line does not end with CRLF")
        return line

    @staticmethod
    def _decode_line(line: bytes, error_message: str) -> str:
        try:
            return line.decode("utf-8").rstrip("\r\n")
        except UnicodeDecodeError as exc:
            raise HttpParseError(error_message) from exc
