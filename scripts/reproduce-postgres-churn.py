#!/usr/bin/env python3
"""Reproduce short-circuit connection churn with synthetic data and local Docker.

Usage: python3 scripts/reproduce-postgres-churn.py --binary ./dist/openfga
Requires Python 3.11+, Docker and a native OpenFGA binary; no Python packages.
"""

import argparse
import asyncio
import hashlib
import json
import os
import socket
import tempfile
import time
import urllib.parse
import uuid
from pathlib import Path


def object_type(name, relation):
    return {
        "type": name,
        "relations": {relation: {"this": {}}},
        "metadata": {
            "relations": {
                relation: {
                    "directly_related_user_types": [
                        {"type": "user"},
                        {"type": "role", "relation": "assignee"},
                    ],
                },
            }
        },
    }


MODEL = {
    "schema_version": "1.1",
    "type_definitions": [
        {"type": "user"},
        object_type("role", "assignee"),
        object_type("resource", "viewer"),
    ],
}


def tuple_key(user, relation, obj):
    return {"user": user, "relation": relation, "object": obj}


async def command(*args, check=True, **kwargs):
    process = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        **kwargs,
    )
    try:
        output, _ = await asyncio.wait_for(process.communicate(), 60)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    if check and process.returncode:
        raise RuntimeError(f"{args[0]} failed: {output.decode()}")
    return output.decode().strip()


async def request(url, body=None):
    # HTTP/1.0 closes the response, avoiding a client dependency or chunk parser.
    parsed = urllib.parse.urlsplit(url)
    data = b"" if body is None else json.dumps(body).encode()
    method = "GET" if body is None else "POST"
    async with asyncio.timeout(5):
        reader, writer = await asyncio.open_connection(parsed.hostname, parsed.port)
        try:
            writer.write(
                (
                    f"{method} {parsed.path} HTTP/1.0\r\nHost: {parsed.netloc}\r\n"
                    f"Content-Type: application/json\r\nContent-Length: {len(data)}\r\n\r\n"
                ).encode()
                + data
            )
            await writer.drain()
            headers, response = (await reader.read()).split(b"\r\n\r\n", 1)
            if int(headers.split(b" ", 2)[1]) >= 400:
                raise RuntimeError(f"HTTP request failed: {headers.decode()} {response.decode()}")
            return response.decode()
        finally:
            writer.close()
            await writer.wait_closed()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def run(args):
    binary = str(Path(args.binary).resolve(strict=True))
    with open(binary, "rb") as executable:
        digest = hashlib.file_digest(executable, "sha256").hexdigest()
    container = "openfga-churn-" + uuid.uuid4().hex
    server = proxy = None
    tasks = set()
    api, metrics, grpc = (free_port() for _ in range(3))

    async def relay(reader, writer):
        async def copy(source, destination, delay=0):
            while data := await source.read(65536):
                if delay:
                    await asyncio.sleep(delay)
                destination.write(data)
                await destination.drain()
            destination.write_eof()

        upstream = None
        task = asyncio.current_task()
        tasks.add(task)
        try:
            remote, upstream = await asyncio.open_connection("127.0.0.1", db_port)
            async with asyncio.TaskGroup() as group:
                group.create_task(copy(reader, upstream))
                group.create_task(copy(remote, writer, args.delay_ms / 1000))
        except* OSError:
            pass  # Cancelled PostgreSQL connections are expected in this experiment.
        finally:
            writer.close()
            if upstream:
                upstream.close()
            tasks.remove(task)

    async def pool():
        text = await request(f"http://127.0.0.1:{metrics}/metrics")
        return {
            line.split()[0].split("{")[0]: float(line.split()[1])
            for line in text.splitlines()
            if line.startswith("pgxpool_")
        }

    with tempfile.TemporaryDirectory(prefix="openfga-churn-") as directory:
        with open(Path(directory) / "server.log", "wb") as log:
            try:
                await command(
                    "docker",
                    "run",
                    "--rm",
                    "-d",
                    "--name",
                    container,
                    "-p",
                    "127.0.0.1::5432",
                    "-e",
                    "POSTGRES_PASSWORD=synthetic-only",
                    args.postgres_image,
                )
                image_id = await command("docker", "inspect", "--format", "{{.Image}}", container)
                db_port = int((await command("docker", "port", container, "5432/tcp")).rsplit(":", 1)[1])
                for _ in range(60):
                    try:
                        await command("docker", "exec", container, "pg_isready", "-U", "postgres")
                        break
                    except RuntimeError:
                        await asyncio.sleep(0.5)
                else:
                    raise TimeoutError("Local PostgreSQL did not become ready")
                proxy = await asyncio.start_server(relay, "127.0.0.1", 0)
                proxy_port = proxy.sockets[0].getsockname()[1]
                uri = "postgres://postgres:synthetic-only@127.0.0.1:{}/postgres?sslmode=disable"
                settings = {
                    "DATASTORE_ENGINE": "postgres",
                    "DATASTORE_URI": uri.format(proxy_port),
                    "DATASTORE_MAX_OPEN_CONNS": "20",
                    "DATASTORE_METRICS_ENABLED": "true",
                    "CONTEXT_PROPAGATION_TO_DATASTORE": "true",
                    "REQUEST_TIMEOUT": "3s",
                    "HTTP_UPSTREAM_TIMEOUT": "5s",
                    "AUTHN_METHOD": "none",
                    "HTTP_ADDR": f"127.0.0.1:{api}",
                    "GRPC_ADDR": f"127.0.0.1:{grpc}",
                    "METRICS_ENABLED": "true",
                    "METRICS_ADDR": f"127.0.0.1:{metrics}",
                    "PLAYGROUND_ENABLED": "false",
                    "PROFILER_ENABLED": "false",
                    "TRACE_ENABLED": "false",
                    "CHECK_QUERY_CACHE_ENABLED": "true",
                    "CHECK_QUERY_CACHE_TTL": "10s",
                    "CHECK_ITERATOR_CACHE_ENABLED": "true",
                    "CHECK_ITERATOR_CACHE_TTL": "10s",
                    "SHARED_ITERATOR_ENABLED": "true",
                }
                env = {k: v for k, v in os.environ.items() if not k.startswith("OPENFGA_")}
                env.update({"OPENFGA_" + k: v for k, v in settings.items()})
                await command(binary, "migrate", env=env | {"OPENFGA_DATASTORE_URI": uri.format(db_port)})
                server = await asyncio.create_subprocess_exec(binary, "run", env=env, stdout=log, stderr=log)
                base = f"http://127.0.0.1:{api}"
                for _ in range(60):
                    if server.returncode is not None:
                        raise RuntimeError(Path(log.name).read_text())
                    try:
                        await request(base + "/healthz")
                        break
                    except (OSError, RuntimeError):
                        await asyncio.sleep(0.5)
                else:
                    raise TimeoutError("Local OpenFGA did not become ready")
                store = json.loads(await request(base + "/stores", {"name": "synthetic-churn"}))["id"]
                base += f"/stores/{store}"
                model = json.loads(await request(base + "/authorization-models", MODEL))["authorization_model_id"]
                tuples = [tuple_key(f"user:u{i}", "assignee", "role:group") for i in range(32)]
                tuples += [
                    tuple_key(user, "viewer", f"resource:r{i}")
                    for i in range(16)
                    for user in ("role:group#assignee", "user:direct")
                ]
                await request(base + "/write", {"authorization_model_id": model, "writes": {"tuple_keys": tuples}})

                async def check(user, relation, obj, expected):
                    started = time.monotonic()
                    body = json.loads(
                        await request(
                            base + "/check",
                            {
                                "authorization_model_id": model,
                                "tuple_key": tuple_key(user, relation, obj),
                            },
                        )
                    )
                    return {"ms": 1000 * (time.monotonic() - started), "matches": body.get("allowed") is expected}

                before = await pool()
                target, warmup = [], []
                for i in range(16):
                    obj = f"resource:r{i}"
                    warmup.append(await check("user:outsider", "viewer", obj, False))
                    warmup += await asyncio.gather(
                        *(check(f"user:u{j}", "assignee", "role:group", True) for j in range(32))
                    )
                    target += await asyncio.gather(
                        *(
                            check(user, "viewer", obj, expected)
                            for user, expected in [
                                *[(f"user:u{j}", True) for j in range(32)],
                                ("user:direct", True),
                                ("user:denied", False),
                            ]
                        )
                    )
                for _ in range(100):
                    after = await pool()
                    if after["pgxpool_acquired_conns"] == after["pgxpool_constructing_conns"] == 0:
                        break
                    await asyncio.sleep(0.1)
                else:
                    raise TimeoutError("SQL pool did not drain within ten seconds")
                delta = {key: after[key] - before[key] for key in before}
                durations = sorted(row["ms"] for row in target)
                result = {
                    "binary_sha256": digest,
                    "postgres_image": args.postgres_image,
                    "postgres_image_id": image_id,
                    "reply_delay_ms": args.delay_ms,
                    "checks": len(target),
                    "warmup_checks": len(warmup),
                    "mismatches": sum(not row["matches"] for row in target + warmup),
                    "p95_ms": durations[int(0.95 * (len(durations) - 1))],
                    "replacement_estimate": delta["pgxpool_new_conns_count"] - delta["pgxpool_total_conns"],
                    "pool_before": before,
                    "pool_after": after,
                }
            finally:
                if server and server.returncode is None:
                    server.terminate()
                    try:
                        await asyncio.wait_for(server.wait(), 10)
                    except TimeoutError:
                        server.kill()
                        await server.wait()
                if proxy:
                    proxy.close()
                for task in list(tasks):
                    task.cancel()
                await asyncio.gather(*list(tasks), return_exceptions=True)
                if proxy:
                    await proxy.wait_closed()
                await command("docker", "rm", "-f", container, check=False)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, help="Native OpenFGA executable to test")
    parser.add_argument("--delay-ms", type=int, choices=(0, 2, 10, 40), default=2)
    parser.add_argument("--postgres-image", default="postgres:17-alpine")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = asyncio.run(run(args))
    text = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(text)
    print(text, end="")
    raise SystemExit(bool(result["mismatches"]))
