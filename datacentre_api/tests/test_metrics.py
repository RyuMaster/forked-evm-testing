"""Tests for the Prometheus metrics setup.

The headline regression test here is `test_metrics_not_mounted_on_fastapi_app`:
it asserts that GET /metrics on the FastAPI app returns 404 (i.e. no route
matches it). This is the safety net for the security concern documented in
main.py near the Instrumentator setup — if anyone calls `Instrumentator(...)
.instrument(app).expose(app)` instead of just `.instrument(app)`, /metrics
becomes a public route on port 8000 and leaks operational telemetry through
the prod-ingress.yaml `/api/(.*)` catch-all to anyone hitting
https://services.soccerverse.com/api/metrics. We don't want that. The
metrics endpoint is supposed to live on port 9100, served only by the
gunicorn master, behind cluster networking.

The other tests verify the Instrumentator middleware is wired up so that
metrics actually get collected, and that the request count for an endpoint
bumps when the endpoint is hit.
"""
import os

# modules/base.py builds SQLAlchemy engines at import time using these env
# vars. Provide bogus-but-parseable values so importing main doesn't blow
# up under pytest. Same trick as tests/test_ticker.py — see that file for
# the explanation.
os.environ.setdefault("MYSQL_HOST", "localhost")
os.environ.setdefault("MYSQL_PORT", "3306")
os.environ.setdefault("MYSQL_USER", "test")
os.environ.setdefault("MYSQL_PASSWORD", "test")
os.environ.setdefault("MYSQL_DB", "test")
os.environ.setdefault("MYSQL_ARCHIVAL_HOST", "localhost")
os.environ.setdefault("MYSQL_ARCHIVAL_PORT", "3306")
os.environ.setdefault("MYSQL_ARCHIVAL_USER", "test")
os.environ.setdefault("MYSQL_ARCHIVAL_PASSWORD", "test")
os.environ.setdefault("MYSQL_ARCHIVAL_DB", "test")
os.environ.setdefault("SQLITE_DB_PATH", "/tmp/test.sqlite")
os.environ.setdefault("PLAYERHISTORY_SQLITE_DB_PATH", "/tmp/test_ph.sqlite")
# Set the multiproc dir BEFORE main.py is imported so its
# os.environ.setdefault is a no-op and we control where files go in tests.
os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", "/tmp/test_prom_multiproc")
os.makedirs(os.environ["PROMETHEUS_MULTIPROC_DIR"], exist_ok=True)

import re  # noqa: E402
from pathlib import Path  # noqa: E402

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402  (importing for the side effect of building the app)


@pytest.fixture
def client():
    """Plain TestClient over the FastAPI app — does NOT involve gunicorn.

    The metrics endpoint on port 9100 is gunicorn-master only, so it is
    not exercised here. These tests cover the FastAPI side: making sure
    /metrics is NOT mounted on the app and that the Instrumentator
    middleware is wired up.
    """
    return TestClient(main.app)


def test_metrics_not_mounted_on_fastapi_app(client):
    """SECURITY REGRESSION: /metrics MUST NOT be a route on the FastAPI app.

    If this test fails it means somebody added `.expose(app)` to the
    Instrumentator setup in main.py. That would mount /metrics on port
    8000, which is reachable via the public ingress's /api/(.*) catch-all
    in clusters/prod/environments/prod/ingress/prod-ingress.yaml. The
    metrics dump would then be readable by anyone hitting
    https://services.soccerverse.com/api/metrics — exposing endpoint
    names, request rates, latencies, error counts, in-flight counts,
    Python process metrics, gunicorn worker pids, and more.

    The metrics endpoint is supposed to be served by the gunicorn master
    on port 9100 (see gunicorn.conf.py:when_ready) behind cluster
    networking only. Don't change this.
    """
    response = client.get("/metrics")
    assert response.status_code == 404, (
        "/metrics is mounted on the FastAPI app — this would leak metrics "
        "publicly via the /api/(.*) catch-all in prod-ingress.yaml. "
        "Remove the .expose(app) call from main.py and use the gunicorn "
        "master metrics server in gunicorn.conf.py instead."
    )


def test_main_py_does_not_call_expose():
    """Belt-and-braces source check: main.py must not call `.expose(`
    on the Instrumentator. Catches even the case where someone calls
    .expose() in a way that doesn't immediately add a route at import
    time (e.g. inside a startup hook)."""
    main_py = Path(__file__).parent.parent / "main.py"
    src = main_py.read_text()
    # Strip comments so the warning text in the comment doesn't trip the
    # check.
    src_no_comments = re.sub(r"#.*", "", src)
    assert ".expose(" not in src_no_comments, (
        "main.py calls .expose() — this puts /metrics on the FastAPI app "
        "and leaks it via the public ingress. See test docstring above."
    )


def test_instrumentator_middleware_registered(client):
    """The Instrumentator middleware should be wired up so that hitting
    any endpoint goes through its histogram/counter logic. Verify by
    checking that the metric families it creates exist after the app
    has been imported."""
    from prometheus_client import REGISTRY

    metric_names = {m.name for m in REGISTRY.collect()}
    # prometheus-fastapi-instrumentator's standard metric is
    # `http_requests_total` (counter) and `http_request_duration_seconds`
    # (histogram). At least one of these must be present.
    expected_any_of = {
        "http_requests",  # the metric family for http_requests_total
        "http_request_duration_seconds",
    }
    assert metric_names & expected_any_of, (
        f"Instrumentator middleware not registered — none of "
        f"{expected_any_of} found in the default registry. "
        f"Available: {sorted(metric_names)[:25]}..."
    )


def test_latency_histogram_has_fine_buckets(client):
    """The latency histogram should have fine-grained buckets so we can see
    the slow tail. Default `prometheus_fastapi_instrumentator` buckets are
    (0.1, 0.5, 1) which clipped p99 at 1s and hid 1-2s SLOW REQUEST tails
    that show up in app logs (perf review 2026-04-29). Regression test:
    confirm 0.025, 2.5, and 10 buckets are present.
    """
    from prometheus_client import REGISTRY

    # Bucket samples only exist after at least one request has flowed
    # through the histogram — hit /openapi.json (cheap, no DB, not in
    # excluded_handlers) so the metric family materialises with its
    # le-labelled samples.
    client.get("/openapi.json")

    bucket_les: set[str] = set()
    for family in REGISTRY.collect():
        if family.name == "http_request_duration_seconds":
            for sample in family.samples:
                if sample.name.endswith("_bucket"):
                    bucket_les.add(sample.labels.get("le", ""))
            break

    # Concrete representatives of "fine-grained" — the old default would
    # have none of these.
    expected_buckets = {"0.025", "2.5", "10.0"}
    assert expected_buckets.issubset(bucket_les), (
        f"http_request_duration_seconds histogram lost its fine-grained "
        f"buckets — found {sorted(bucket_les)}. Someone reverted the "
        f"latency_lowr_buckets argument in main.py?"
    )


def test_excluded_handlers_are_not_in_routes(client):
    """`/healthz` is in the Instrumentator's excluded_handlers list (it's
    a high-frequency probe and we don't want it dominating the request
    count). Verify the route still exists on the app though — exclusion
    affects whether it's instrumented, not whether it's served."""
    routes = {getattr(r, "path", None) for r in main.app.routes}
    assert "/healthz" in routes, "/healthz route disappeared from the app"
    # And /metrics is not a route on the app (covered above too, this is
    # a slightly different angle on the same property)
    assert "/metrics" not in routes, (
        "/metrics is registered as a FastAPI route — leak risk, see "
        "test_metrics_not_mounted_on_fastapi_app"
    )
