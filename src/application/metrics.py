import threading
import time


class Metrics:
    def __init__(self) -> None:
        self._start_time = time.monotonic()
        self._requests_total = 0
        self._errors_total = 0
        self._active_connections = 0
        self._request_duration_seconds_sum = 0.0
        self._request_duration_seconds_count = 0
        self._lock = threading.Lock()

    def inc_requests(self) -> None:
        with self._lock:
            self._requests_total += 1

    def inc_errors(self) -> None:
        with self._lock:
            self._errors_total += 1

    def inc_active_connections(self) -> None:
        with self._lock:
            self._active_connections += 1

    def dec_active_connections(self) -> None:
        with self._lock:
            if self._active_connections > 0:
                self._active_connections -= 1

    def observe_request_duration(self, seconds: float) -> None:
        with self._lock:
            self._request_duration_seconds_sum += seconds
            self._request_duration_seconds_count += 1

    def snapshot(self) -> dict[str, float]:
        with self._lock:
            uptime = max(0.0, time.monotonic() - self._start_time)
            return {
                "requests_total": float(self._requests_total),
                "errors_total": float(self._errors_total),
                "active_connections": float(self._active_connections),
                "request_duration_seconds_sum": self._request_duration_seconds_sum,
                "request_duration_seconds_count": float(self._request_duration_seconds_count),
                "uptime_seconds": uptime,
            }

    def render_prometheus(self) -> str:
        metrics = self.snapshot()
        lines = [
            "# TYPE mini_nginx_requests_total counter",
            f"mini_nginx_requests_total {metrics['requests_total']}",
            "# TYPE mini_nginx_errors_total counter",
            f"mini_nginx_errors_total {metrics['errors_total']}",
            "# TYPE mini_nginx_active_connections gauge",
            f"mini_nginx_active_connections {metrics['active_connections']}",
            "# TYPE mini_nginx_request_duration_seconds_sum counter",
            f"mini_nginx_request_duration_seconds_sum {metrics['request_duration_seconds_sum']}",
            "# TYPE mini_nginx_request_duration_seconds_count counter",
            f"mini_nginx_request_duration_seconds_count {metrics['request_duration_seconds_count']}",
            "# TYPE mini_nginx_uptime_seconds gauge",
            f"mini_nginx_uptime_seconds {metrics['uptime_seconds']}",
        ]
        return "\n".join(lines) + "\n"
