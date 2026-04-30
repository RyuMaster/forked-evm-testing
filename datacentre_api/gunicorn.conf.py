"""Gunicorn configuration for datacentre_api.

The bulk of the gunicorn invocation is still on the command line in the
deployment YAMLs (-w 4 -k uvicorn.workers.UvicornWorker etc.). This file
exists primarily to wire up the Prometheus metrics multiprocess server.

## Metrics architecture

The /metrics endpoint is served by the gunicorn MASTER process on port
9100, NOT by FastAPI on port 8000. This is deliberate:

- The K8s Service for this app exposes both 8000 (api) and 9100 (metrics)
- The public nginx ingress (prod-ingress.yaml) only forwards /api/* to
  port 8000 — port 9100 is unreachable from outside the cluster
- Prometheus's ServiceMonitor scrapes 9100 directly via cluster
  networking, never via the public ingress

The master process binds 9100 in a daemon thread (via prometheus_client's
start_http_server). Worker processes do NOT bind 9100 — they only write
metric updates to files in PROMETHEUS_MULTIPROC_DIR. The master serves
the aggregated view by reading those files via MultiProcessCollector
on every scrape request.

To use this file, add `-c gunicorn.conf.py` to the gunicorn command in
the deployment YAML. The file is copied into the container at
/usr/src/app/gunicorn.conf.py by the standard `COPY . .` in
docker/Dockerfile.
"""
import os

# Ensure the multiproc dir exists. main.py also does this but the master
# loads this conf file BEFORE main.py is imported, so it has to live here
# too. Workers inherit the env var via the master's environment.
os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus_multiproc")
os.makedirs(os.environ["PROMETHEUS_MULTIPROC_DIR"], exist_ok=True)

# Where the master binds the metrics endpoint. Override via env var if
# the default port collides with anything else in the pod.
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9100"))


def when_ready(server):
    """Master process: start the Prometheus metrics HTTP server before
    workers fork. Reads aggregated metrics from PROMETHEUS_MULTIPROC_DIR
    via MultiProcessCollector so all 4 worker processes' contributions
    are visible in a single scrape.
    """
    from prometheus_client import CollectorRegistry, multiprocess, start_http_server

    # Clean any stale files left by a previous container start. Without
    # this, counters from a dead worker process can show up as ghost
    # series after a restart.
    multiproc_dir = os.environ["PROMETHEUS_MULTIPROC_DIR"]
    for fname in os.listdir(multiproc_dir):
        try:
            os.remove(os.path.join(multiproc_dir, fname))
        except OSError:
            pass

    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    start_http_server(METRICS_PORT, registry=registry)
    server.log.info(
        "prometheus metrics endpoint started on port %s "
        "(multiproc dir=%s)",
        METRICS_PORT,
        os.environ["PROMETHEUS_MULTIPROC_DIR"],
    )


def child_exit(server, worker):
    """When a worker exits, mark its metrics files as dead so the
    multiproc collector cleans them up. Without this, dead workers leave
    behind stale counter files that get summed into every scrape until
    the next master restart.
    """
    from prometheus_client import multiprocess
    multiprocess.mark_process_dead(worker.pid)
