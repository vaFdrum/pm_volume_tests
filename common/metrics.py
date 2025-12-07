"""Prometheus metrics for load testing monitoring"""

import time

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from config import CONFIG

# Counters
REQUEST_COUNT = Counter(
    "superset_loadtest_requests_total",
    "Total number of requests",
    ["method", "endpoint", "status"],
)

AUTH_ATTEMPTS = Counter(
    "superset_loadtest_auth_attempts_total",
    "Total authentication attempts",
    ["username", "success"],
)

CHUNK_UPLOADS = Counter(
    "superset_loadtest_chunk_uploads_total",
    "Total chunk uploads",
    ["flow_id", "status"],
)

FLOW_CREATIONS = Counter(
    "superset_loadtest_flow_creations_total", "Total flow creations", ["status"]
)

# Gauges
ACTIVE_USERS = Gauge(
    "superset_loadtest_active_users", "Number of currently active users"
)

CHUNKS_IN_PROGRESS = Gauge(
    "superset_loadtest_chunks_in_progress", "Number of chunks currently being uploaded"
)

UPLOAD_PROGRESS = Gauge(
    "superset_loadtest_upload_progress_percent",
    "Upload progress percentage",
    ["flow_id"],
)

SESSION_STATUS = Gauge(
    "superset_loadtest_session_status",
    "User session status (1=active, 0=inactive)",
    ["username"],
)

# Histograms
REQUEST_DURATION = Histogram(
    "superset_loadtest_request_duration_seconds",
    "Request duration in seconds",
    ["method", "endpoint"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
)

AUTH_DURATION = Histogram(
    "superset_loadtest_auth_duration_seconds",
    "Authentication duration in seconds",
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0],
)

CHUNK_UPLOAD_DURATION = Histogram(
    "superset_loadtest_chunk_upload_duration_seconds",
    "Chunk upload duration in seconds",
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)

FLOW_PROCESSING_DURATION = Histogram(
    "superset_loadtest_flow_processing_duration_seconds",
    "Flow processing duration in seconds",
    ["flow_id"],
)

# Database metrics
COUNT_VALIDATION_RESULT = Gauge(
    "superset_loadtest_COUNT_VALIDATION_RESULT",
    "Database count validation result (1=success, 0=failure)",
    ["flow_id"],
)

DB_ROW_COUNT = Gauge(
    "superset_loadtest_db_row_count", "Number of rows in target table", ["flow_id"]
)

EXPECTED_ROWS = Gauge(
    "superset_loadtest_expected_rows", "Expected number of rows from CSV"
)


def start_metrics_server(port=9090):
    """Start Prometheus metrics server"""
    if CONFIG.get("enable_metrics", False):
        start_http_server(port)
        print(f"Prometheus metrics server started on port {port}")


class MetricsMiddleware:
    """Middleware for tracking request metrics"""

    def __init__(self, client):
        self.client = client

    def track_request(self, method, url, name, **kwargs):
        """Track request metrics"""
        start_time = time.time()

        try:
            response = method(url, name=name, **kwargs)
            duration = time.time() - start_time

            # Record metrics
            REQUEST_DURATION.labels(
                method=method.__name__.upper(), endpoint=name
            ).observe(duration)

            REQUEST_COUNT.labels(
                method=method.__name__.upper(),
                endpoint=name,
                status=response.status_code if response else "error",
            ).inc()

            return response

        except Exception as e:
            duration = time.time() - start_time
            REQUEST_DURATION.labels(
                method=method.__name__.upper(), endpoint=name
            ).observe(duration)

            REQUEST_COUNT.labels(
                method=method.__name__.upper(), endpoint=name, status="error"
            ).inc()

            raise e
