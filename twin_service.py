"""MQTT -> bounded queue -> event-time pipeline -> state subscribers + InfluxDB."""
import argparse
import csv
import json
import os
import queue
import threading
import time
from pathlib import Path
import paho.mqtt.client as mqtt
from mqtt_common import validate, validate_status
from twin_state import TwinPipeline, iso
from timeseries_store import HistoryStore


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=1883)
    p.add_argument('--device-id', default='mannequin-01')
    p.add_argument('--client-id', default='mannequin-twin-lab3')
    p.add_argument('--output', type=Path, default=Path('results/live'))
    p.add_argument('--duration', type=float, default=0)
    p.add_argument('--period-ms', type=float, default=100)
    p.add_argument('--reorder-ms', type=float, default=100)
    p.add_argument('--angle-threshold', type=float, default=1)
    p.add_argument('--influx-url', default=os.getenv('INFLUX_URL', 'http://127.0.0.1:8086'))
    p.add_argument('--influx-org', default=os.getenv('INFLUX_ORG', 'dt-lab'))
    p.add_argument('--influx-bucket', default=os.getenv('INFLUX_BUCKET', 'mannequin'))
    p.add_argument('--local-only', action='store_true', help='Лише журнали й outbox; не перевірка InfluxDB')
    p.add_argument('--pause-at', type=float, default=0, help='Тест: відключити підписника через N с')
    p.add_argument('--pause-seconds', type=float, default=3)
    args = p.parse_args()
    if args.duration < 0 or args.period_ms <= 0 or args.reorder_ms < 0 or args.pause_seconds < 0:
        p.error('Некоректні часові параметри')
    token = os.getenv('INFLUX_TOKEN')
    if not args.local_only and not token:
        p.error('Задайте INFLUX_TOKEN або явно використайте --local-only')
    args.output.mkdir(parents=True, exist_ok=True)
    store = HistoryStore(args.output, None if args.local_only else args.influx_url,
                         args.influx_org, args.influx_bucket, token)
    inbox = queue.Queue(maxsize=50000)
    ready = threading.Event()
    stats = {'callback_overflow': 0, 'invalid': 0, 'notification_failed': 0}
    # Фіксований client_id + persistent session: брокер може накопичити QoS1
    # під час тимчасового розриву. ACK після передачі в ядро, а не callback.
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=args.client_id,
                         clean_session=False, manual_ack=True)
    if os.getenv('MQTT_USERNAME'):
        client.username_pw_set(os.environ['MQTT_USERNAME'], os.getenv('MQTT_PASSWORD'))
    client.max_queued_messages_set(5000)
    client.reconnect_delay_set(1, 5)
    metric_file = (args.output / 'latency.csv').open('w', encoding='utf-8', newline='')
    metric_writer = csv.DictWriter(metric_file, fieldnames=['device_id', 'run_id', 'sample_id',
        'sensor_id', 'generated_at', 'received_at', 'updated_at', 'state_version', 'latency_ms', 'clock_valid', 'representation'])
    metric_writer.writeheader()

    def state_changed(state):
        store.state(state)
        info = client.publish(f'dt/{args.device_id}/state', json.dumps(state, allow_nan=False),
                              qos=1, retain=True)
        # NO_CONN за QoS1 означає локальну чергу Paho, її вміст не є
        # підтвердженою доставкою. Остаточну доставку перевіряє state_subscriber.
        if info.rc not in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
            stats['notification_failed'] += 1

    pipeline = TwinPipeline(args.device_id, args.period_ms/1000, args.reorder_ms/1000,
                            args.angle_threshold, state_changed, store.raw, metric_writer.writerow, clock=time.time)

    def enqueue(item):
        try:
            inbox.put_nowait(item)
        except queue.Full:
            stats['callback_overflow'] += 1
            # Без ACK: повідомлення не вважається завершеним на рівні MQTT.

    def connected(c, u, flags, code, properties):
        if not code.is_failure:
            c.subscribe([(f'dt/{args.device_id}/telemetry', 1),
                         (f'dt/{args.device_id}/events', 1),
                         (f'dt/{args.device_id}/status', 1)])
            enqueue(('broker', True, time.time()))

    def subscribed(c, u, mid, codes, properties):
        if all(not code.is_failure for code in codes):
            ready.set()

    def disconnected(c, u, flags, code, properties):
        ready.clear()
        enqueue(('broker', False, time.time()))

    def message(c, u, msg):
        enqueue(('message', (msg.topic, bytes(msg.payload), msg.mid, msg.qos, bool(msg.retain)), time.time()))

    client.on_connect, client.on_subscribe = connected, subscribed
    client.on_disconnect, client.on_message = disconnected, message
    started = time.monotonic()
    paused = False
    resume_at = None
    next_flush = started
    failure = None
    try:
        client.connect(args.host, args.port, keepalive=5)
        client.loop_start()
        if not ready.wait(10):
            raise RuntimeError('MQTT SUBACK timeout')
        started = time.monotonic()
        print('READY | persistent MQTT session | ' + str(args.output), flush=True)
        while not args.duration or time.monotonic() - started < args.duration:
            elapsed = time.monotonic() - started
            if args.pause_at and not paused and elapsed >= args.pause_at:
                paused = True
                client.disconnect()
                resume_at = time.monotonic() + args.pause_seconds
                print('TEST | subscriber disconnected', flush=True)
            if resume_at is not None and time.monotonic() >= resume_at:
                client.reconnect()
                client.loop_stop()
                client.loop_start()
                resume_at = None
                print('TEST | subscriber reconnecting', flush=True)
            # Обмежений batch не дозволяє сплеску нескінченно відкладати timer.
            for _ in range(2000):
                try:
                    kind, value, received = inbox.get_nowait()
                except queue.Empty:
                    break
                if kind == 'broker':
                    pipeline.broker_status(value, received)
                    continue
                topic, payload, mid, qos, retained = value
                if topic not in {f'dt/{args.device_id}/telemetry', f'dt/{args.device_id}/events',
                                 f'dt/{args.device_id}/status'}:
                    stats['unexpected_topic'] = stats.get('unexpected_topic', 0) + 1
                    if qos:
                        client.ack(mid, qos)
                    continue
                try:
                    if topic.endswith('/status'):
                        pipeline.status(validate_status(topic, payload), time.time(), retained)
                    else:
                        pipeline.ingest(validate(topic, payload), received)
                except (ValueError, OverflowError, RecursionError) as exc:
                    stats['invalid'] += 1
                    store.journal('errors', dict(received_at=iso(received), topic=topic,
                                                error=str(exc), payload=payload[:1024].decode(errors='replace')))
                if qos:
                    client.ack(mid, qos)
            pipeline.advance(time.time())
            if time.monotonic() >= next_flush:
                store.flush()
                metric_file.flush()
                next_flush = time.monotonic() + .2
            time.sleep(.005)
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        failure = str(exc)
        raise
    finally:
        # Джерело слід завершити раніше за сервіс; залишок reorder-буфера
        # витримує своє вікно, а не отримує штучний час оновлення в майбутньому.
        deadline = time.monotonic() + args.reorder_ms / 1000 + .15
        while pipeline.heap and time.monotonic() < deadline:
            pipeline.advance(time.time())
            time.sleep(.01)
        client.on_disconnect = None
        client.disconnect()
        client.loop_stop()
        metric_file.close()
        store_result = store.close()
        summary = dict(**stats, **dict(pipeline.counts), **store_result,
                       elapsed_s=time.monotonic()-started, buffer_remaining=len(pipeline.heap),
                       inbox_remaining=inbox.qsize(), failure=failure,
                       parameters=vars(args) | {'output': str(args.output)},
                       restart_guarantee='No process-crash exactly-once guarantee; dedup/state are in memory')
        (args.output / 'service_summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
        print(json.dumps(summary, indent=2), flush=True)


if __name__ == '__main__':
    main()
