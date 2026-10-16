import argparse
import csv
import json
import math
import threading
import time
import uuid
from pathlib import Path

import paho.mqtt.client as mqtt
from mqtt_common import Connection, FIELDS, utc_now, validate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=1883)
    parser.add_argument('--qos', type=int, choices=(0, 1, 2), default=1)
    parser.add_argument('--duration', type=float, default=0,
                        help='Реальні секунди після підписки; 0 = до Ctrl+C')
    parser.add_argument('--log-dir', type=Path, default=Path('logs'))
    args = parser.parse_args()
    if not math.isfinite(args.duration) or args.duration < 0:
        parser.error('duration має бути скінченним невід’ємним числом')
    args.log_dir.mkdir(parents=True, exist_ok=True)
    conn = Connection('sub-' + uuid.uuid4().hex[:16])
    subscribed = threading.Event()
    failures = []
    counts = {'valid': 0, 'invalid': 0, 'duplicate': 0}
    seen = set()  # Унікальність: (device_id, run_id, sample_id).
    received_path = args.log_dir / 'received.csv'
    errors_path = args.log_dir / 'errors.jsonl'
    fields = ['received_at', 'topic', 'received_qos'] + sorted(FIELDS)
    # Файли доповнюються; дедуплікація діє в межах поточного процесу.
    needs_header = not received_path.exists() or received_path.stat().st_size == 0
    with received_path.open('a', newline='', encoding='utf-8-sig') as good, \
            errors_path.open('a', encoding='utf-8') as bad:
        writer = csv.DictWriter(good, fieldnames=fields)
        if needs_header:
            writer.writeheader()
            good.flush()

        def on_connect(client, userdata, flags, reason_code, properties):
            conn.on_connect(client, userdata, flags, reason_code, properties)
            if not reason_code.is_failure:
                subscribed.clear()
                rc, _ = client.subscribe([('dt/+/telemetry', args.qos), ('dt/+/events', args.qos)])
                if rc != mqtt.MQTT_ERR_SUCCESS:
                    failures.append(f'SUBSCRIBE: {rc}')

        def on_subscribe(client, userdata, mid, reason_codes, properties):
            if any(code.is_failure for code in reason_codes):
                failures.append('Брокер відхилив підписку')
            else:
                subscribed.set()

        def on_message(client, userdata, message):
            received_at = utc_now()
            try:
                try:
                    data = validate(message.topic, message.payload)
                except (ValueError, OverflowError, RecursionError) as exc:
                    counts['invalid'] += 1
                    bad.write(json.dumps({'received_at': received_at, 'topic': message.topic,
                                          'error': str(exc),
                                          'payload': message.payload[:4096].decode('utf-8', errors='replace')},
                                         ensure_ascii=False) + '\n')
                    bad.flush()
                    print(f'REJECTED | {message.topic} | {exc}', flush=True)
                    return
                key = (data['device_id'], data['run_id'], data['sample_id'])
                if key in seen:
                    counts['duplicate'] += 1
                    return
                writer.writerow({'received_at': received_at, 'topic': message.topic,
                                 'received_qos': message.qos, **data})
                seen.add(key)
                counts['valid'] += 1
            except Exception as exc:
                # Помилку диска/оброблення передаємо основному потоку.
                failures.append(f'Помилка оброблення: {exc}')

        conn.client.on_connect = on_connect
        conn.client.on_subscribe = on_subscribe
        conn.client.on_message = on_message
        try:
            conn.start(args.host, args.port)
            if not subscribed.wait(10) or failures:
                raise RuntimeError('; '.join(failures) or 'Тайм-аут SUBACK')
            print(f'READY | dt/+/telemetry, dt/+/events | QoS={args.qos}', flush=True)
            started = time.monotonic()
            while args.duration == 0 or time.monotonic() - started < args.duration:
                if failures:
                    raise RuntimeError('; '.join(failures))
                time.sleep(1)
                good.flush()
                print('RECEIVED | ' + ' | '.join(f'{k}={v}' for k, v in counts.items()), flush=True)
        except KeyboardInterrupt:
            print('\nЗупинення користувачем.')
        finally:
            conn.close()  # Завершує callback перед закриттям файлів.
            good.flush()
            bad.flush()
        if failures:
            raise RuntimeError('; '.join(failures))
    print(f'DONE | {counts}\nДані: {received_path}\nПомилки: {errors_path}')


if __name__ == '__main__':
    try:
        main()
    except (OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(f'ERROR: {exc}')
