"""Serve the lab 5 scene, stream the processed MQTT state and query lab 3 history."""

import argparse
import asyncio
from contextlib import suppress
import csv
from datetime import datetime, timezone
import io
import json
import math
import os
from pathlib import Path

from aiohttp import ClientError, ClientSession, ClientTimeout, web
import paho.mqtt.client as mqtt

from mqtt_common import identifier

ROOT = Path(__file__).resolve().parent
PERIODS = {"5m": "1s", "15m": "3s", "1h": "12s", "24h": "5m"}
FIELDS = {
    **{f"S-{index:02}": f"S-{index:02}" for index in range(1, 7)},
    "S-07": "drive_current",
    "S-08": "drive_temperature",
    "S-09": "supply_voltage",
}
BENCH_RATES = (10, 30, 60, 120)


class StateFeed:
    def __init__(self, config, loop):
        self.latest = None
        self.listeners = set()
        self.topic = f"dt/{config.device_id}/state"
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if os.getenv("MQTT_USERNAME"):
            self.client.username_pw_set(os.environ["MQTT_USERNAME"], os.getenv("MQTT_PASSWORD"))
        self.client.on_connect = self.subscribe
        self.client.on_disconnect = lambda *_: loop.call_soon_threadsafe(self.update, None)
        self.client.on_message = lambda *_args: loop.call_soon_threadsafe(
            self.receive, _args[-1].payload
        )
        self.client.connect_async(config.mqtt_host, config.mqtt_port, keepalive=15)
        self.client.loop_start()

    def subscribe(self, client, userdata, flags, reason_code, properties):
        if not reason_code.is_failure:
            client.subscribe(self.topic, qos=1)

    def receive(self, payload):
        try:
            state = json.loads(payload)
            if not isinstance(state, dict) or not isinstance(state.get("joint_angles"), dict):
                return
        except (UnicodeDecodeError, ValueError):
            return
        self.update(state)

    def update(self, state):
        self.latest = state
        for queue in self.listeners:
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(state)

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()


async def live_socket(request):
    feed = request.app[FEED]
    queue = asyncio.Queue(maxsize=1)
    socket = web.WebSocketResponse(heartbeat=15)
    await socket.prepare(request)
    feed.listeners.add(queue)
    queue.put_nowait(feed.latest)

    async def send():
        while not socket.closed:
            await socket.send_json(await queue.get())

    sender = asyncio.create_task(send())
    try:
        async for _ in socket:
            pass
    finally:
        feed.listeners.discard(queue)
        sender.cancel()
        with suppress(asyncio.CancelledError, ConnectionError):
            await sender
    return socket


def benchmark_state(index, elapsed, device_id):
    wave = math.sin(elapsed * 2)
    phase = elapsed % 9
    alerts = [] if phase < 3 else ["overcurrent"] if phase < 6 else ["emergency_input_active"]
    return {
        "device_id": device_id,
        "run_id": "benchmark",
        "version": index,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "connection_status": "online",
        "broker_connected": True,
        "joint_angles": {
            "S-01": 22 + 30 * wave, "S-02": 45 + 36 * wave,
            "S-03": 10 + 18 * wave, "S-04": 20 + 16 * wave,
            "S-05": 26 * wave, "S-06": 10 * wave,
        },
        "drive_current": 3.6 if "overcurrent" in alerts else 0.9 + 0.4 * wave,
        "drive_temperature": 40 + 3 * wave,
        "supply_voltage": 24 + 0.4 * wave,
        "limit_switch": False,
        "emergency_input": "emergency_input_active" in alerts,
        "alerts": alerts,
        "sensor_quality": {sensor: {"status": "fresh"} for sensor in FIELDS},
    }


async def benchmark_socket(request):
    rate = request.match_info["rate"]
    if rate not in {str(value) for value in BENCH_RATES}:
        raise web.HTTPNotFound()
    socket = web.WebSocketResponse(heartbeat=15)
    await socket.prepare(request)
    loop = asyncio.get_running_loop()

    async def send():
        start = loop.time()
        index = 0
        while not socket.closed:
            await socket.send_json(benchmark_state(
                index, loop.time() - start, request.app[CONFIG].device_id
            ))
            index += 1
            await asyncio.sleep(max(0, start + index / int(rate) - loop.time()))

    sender = asyncio.create_task(send())
    try:
        async for _ in socket:
            pass
    finally:
        sender.cancel()
        with suppress(asyncio.CancelledError, ConnectionError):
            await sender
    return socket


def history_points(csv_text):
    time_column = value_column = None
    points = []
    for row in csv.reader(io.StringIO(csv_text)):
        if not row or row[0].startswith("#"):
            continue
        if "_time" in row and "_value" in row:
            time_column = row.index("_time")
            value_column = row.index("_value")
        elif time_column is not None and len(row) > max(time_column, value_column):
            timestamp = row[time_column]
            value = float(row[value_column])
            if timestamp and math.isfinite(value):
                points.append([timestamp, value])
    return sorted(points)


async def history(request):
    period = request.query.get("period", "15m")
    sensor = request.query.get("sensor", "S-07")
    if period not in PERIODS or sensor not in FIELDS:
        return web.json_response({"error": "Невідомий датчик або період."}, status=400)
    config = request.app[CONFIG]
    if not config.influx_token:
        return web.json_response({"error": "Сховище історії не налаштовано."}, status=503)

    interval = PERIODS[period]
    query = f'''from(bucket: {json.dumps(config.influx_bucket)})
  |> range(start: -{period})
  |> filter(fn: (r) => r._measurement == "twin_state" and
       r.device_id == {json.dumps(config.device_id)} and r._field == "{FIELDS[sensor]}")
  |> group()
  |> aggregateWindow(every: {interval}, fn: mean, createEmpty: false)
  |> sort(columns: ["_time"])'''
    try:
        async with request.app[HTTP].post(
            f"{config.influx_url.rstrip('/')}/api/v2/query",
            params={"org": config.influx_org},
            headers={"Authorization": f"Token {config.influx_token}", "Accept": "application/csv"},
            json={"query": query, "type": "flux", "dialect": {"annotations": [], "header": True}},
        ) as result:
            result.raise_for_status()
            points = history_points(await result.text())
    except (ClientError, asyncio.TimeoutError, ValueError):
        return web.json_response({"error": "Історія InfluxDB тимчасово недоступна."}, status=503)
    return web.json_response({"points": points, "step_ms": 1000 * (
        int(interval[:-1]) * (60 if interval.endswith("m") else 1)
    )})


CONFIG = web.AppKey("config", argparse.Namespace)
FEED = web.AppKey("feed", StateFeed)
HTTP = web.AppKey("http", ClientSession)


async def resources(app):
    app[FEED] = StateFeed(app[CONFIG], asyncio.get_running_loop())
    async with ClientSession(timeout=ClientTimeout(total=8)) as session:
        app[HTTP] = session
        try:
            yield
        finally:
            app[FEED].close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8095)
    parser.add_argument("--mqtt-host", default=os.getenv("MQTT_HOST", "127.0.0.1"))
    parser.add_argument("--mqtt-port", type=int, default=os.getenv("MQTT_PORT", "1883"))
    parser.add_argument("--device-id", default="mannequin-01")
    config = parser.parse_args()
    if not identifier(config.device_id):
        parser.error("Неприпустимий ідентифікатор об’єкта")
    config.influx_url = os.getenv("INFLUX_URL", "http://127.0.0.1:8086")
    config.influx_org = os.getenv("INFLUX_ORG", "dt-lab")
    config.influx_bucket = os.getenv("INFLUX_BUCKET", "mannequin")
    config.influx_token = os.getenv("INFLUX_TOKEN")
    library = ROOT / "node_modules" / "three"
    if not library.is_dir():
        parser.error("Спочатку виконайте npm ci --prefix lab5")

    app = web.Application()
    app[CONFIG] = config
    app.cleanup_ctx.append(resources)
    app.router.add_get("/", lambda _: web.FileResponse(ROOT / "web" / "index.html"))
    app.router.add_get("/api/history", history)
    app.router.add_get("/ws/state", live_socket)
    app.router.add_get("/ws/benchmark/{rate}", benchmark_socket)
    app.router.add_static("/assets", ROOT / "web")
    app.router.add_static("/vendor/three", library)
    web.run_app(app, host="127.0.0.1", port=config.port)


if __name__ == "__main__":
    main()
