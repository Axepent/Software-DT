import argparse
import csv
import json
import math
import time
import uuid
from collections import deque
from dataclasses import asdict
from pathlib import Path

import paho.mqtt.client as mqtt
from sensor_emulator import SensorEmulator, parse_start
from mqtt_common import Connection, identifier, topic_for, utc_now, validate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=1883)
    parser.add_argument('--qos', type=int, choices=(0, 1, 2), default=1)
    parser.add_argument('--duration', type=float, default=120)
    parser.add_argument('--speed', type=float, default=1)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--device-id', default='mannequin-01')
    parser.add_argument('--start-time', type=parse_start)
    parser.add_argument('--no-anomalies', action='store_true')
    parser.add_argument('--log-dir', type=Path, default=Path('logs'))
    args = parser.parse_args()
    if not math.isfinite(args.duration) or args.duration <= 0:
        parser.error('duration має бути додатним скінченним числом')
    if not math.isfinite(args.speed) or args.speed < 0:
        parser.error('speed має бути невід’ємним скінченним числом')
    if not identifier(args.device_id):
        parser.error('device-id: 1..64 латинських літер, цифр, _ або -')
    run_id = uuid.uuid4().hex
    conn = Connection('pub-' + run_id[:16])
    conn.client.max_inflight_messages_set(100)
    conn.client.max_queued_messages_set(1000)
    args.log_dir.mkdir(parents=True, exist_ok=True)
    path = args.log_dir / f'sent_{run_id}.csv'
    pending = deque()
    count = 0

    def delivered(info):
        info.wait_for_publish(timeout=10)
        if not info.is_published():
            raise RuntimeError('Тайм-аут відправлення; перевірте брокер')

    try:
        conn.start(args.host, args.port)
        print(f'CONNECTED | run_id={run_id} | QoS={args.qos}', flush=True)
        emulator = SensorEmulator(args.seed, args.device_id, args.start_time,
                                  not args.no_anomalies)
        fields = ['topic', 'qos', 'schema_version', 'run_id', 'device_id', 'sample_id',
                  'sensor_id', 'timestamp', 'sim_time_s', 'value', 'unit', 'condition', 'sent_at']
        with path.open('w', newline='', encoding='utf-8-sig') as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            started = time.perf_counter()
            last_print = started
            try:
                for sample in emulator.samples(args.duration):
                    if args.speed:
                        remaining = started + sample.sim_time_s / args.speed - time.perf_counter()
                        if remaining > 0:
                            time.sleep(remaining)
                    if not conn.client.is_connected():
                        raise RuntimeError('Зв’язок із брокером втрачено; прогін перервано')
                    topic = topic_for(sample.device_id, sample.sensor_id)
                    data = {'schema_version': 1, 'run_id': run_id,
                            **asdict(sample), 'sent_at': utc_now()}
                    payload = json.dumps(data, ensure_ascii=False, allow_nan=False)
                    validate(topic, payload.encode('utf-8'))
                    info = conn.client.publish(topic, payload, qos=args.qos, retain=False)
                    if info.rc != mqtt.MQTT_ERR_SUCCESS:
                        raise RuntimeError(f'Помилка publish: {info.rc}')
                    pending.append(info)
                    if len(pending) >= 256:
                        delivered(pending.popleft())
                    # Запис означає прийняту publish спробу; він не є ACK підписника.
                    writer.writerow({'topic': topic, 'qos': args.qos, **data})
                    count += 1
                    if time.perf_counter() - last_print >= 1:
                        print(f'PUBLISHED={count} | model_time={sample.sim_time_s:.2f}s', flush=True)
                        file.flush()
                        last_print = time.perf_counter()
            except KeyboardInterrupt:
                print('\nЗупинення користувачем; очікуємо відправлення черги.')
            finally:
                for info in pending:
                    delivered(info)
        print(f'DONE | sent={count} | run_id={run_id}\nЖурнал: {path}')
        print('Зіставте журнал із received.csv. MQTT ACK не підтверджує запис subscriber.')
    finally:
        conn.close()


if __name__ == '__main__':
    try:
        main()
    except (OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(f'ERROR: {exc}')
