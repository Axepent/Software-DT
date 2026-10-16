import json
import math
import re
import threading
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from sensor_emulator import SENSORS

FIELDS = {'schema_version', 'run_id', 'device_id', 'sample_id', 'sensor_id',
          'timestamp', 'sim_time_s', 'value', 'unit', 'condition', 'sent_at'}
CONDITIONS = {
    'S-07': {'normal', 'overcurrent'},
    'S-08': {'normal', 'overtemperature'},
    'S-09': {'normal', 'voltage_warning'},
    'S-10': {'normal', 'limit_active'},
    'S-11': {'normal', 'emergency_input_active'},
}


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def identifier(value):
    return isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_-]{1,64}', value) is not None


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError('Часова мітка має бути рядком')
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise ValueError('Некоректна часова мітка ISO 8601') from exc
    if dt.tzinfo is None or dt.utcoffset().total_seconds() != 0:
        raise ValueError('Часова мітка має бути в UTC із часовим поясом')
    return dt


def topic_for(device_id, sensor_id):
    channel = 'events' if sensor_id in ('S-10', 'S-11') else 'telemetry'
    return f'dt/{device_id}/{channel}'


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'Повторене поле JSON: {key}')
        result[key] = value
    return result


def validate(topic, payload):
    """Перевірка структури, типів, одиниць, діапазонів і відповідності топіку."""
    if len(payload) > 65536:
        raise ValueError('Повідомлення перевищує 64 KiB')
    try:
        data = json.loads(payload.decode('utf-8'), object_pairs_hook=unique_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError('Очікується UTF-8 JSON') from exc
    if not isinstance(data, dict) or set(data) != FIELDS:
        raise ValueError('Склад полів не відповідає схемі v1')
    if type(data['schema_version']) is not int or data['schema_version'] != 1:
        raise ValueError('Невідома версія схеми')
    for key in ('run_id', 'device_id'):
        if not identifier(data[key]):
            raise ValueError(f'Некоректний {key}')
    if type(data['sample_id']) is not int or data['sample_id'] <= 0:
        raise ValueError('sample_id має бути додатним цілим числом')
    sid = data['sensor_id']
    if not isinstance(sid, str) or sid not in SENSORS:
        raise ValueError('Невідомий sensor_id')
    sensor = SENSORS[sid]
    if topic != topic_for(data['device_id'], sid):
        raise ValueError('Топік не відповідає device_id або типу сигналу')
    if data['unit'] != sensor.unit:
        raise ValueError('Неправильні одиниці вимірювання')
    value = data['value']
    if sensor.period_ticks == 0:
        if type(value) is not bool:
            raise ValueError('Для дискретного сигналу потрібен JSON boolean')
    elif type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError('Потрібне скінченне числове значення')
    elif not sensor.minimum <= value <= sensor.maximum:
        raise ValueError('Значення поза допустимим діапазоном датчика')
    sim_time = data['sim_time_s']
    if type(sim_time) not in (int, float) or not math.isfinite(sim_time) or sim_time < 0:
        raise ValueError('Некоректний модельний час')
    timestamp(data['timestamp'])
    timestamp(data['sent_at'])
    if not isinstance(data['condition'], str) or data['condition'] not in CONDITIONS.get(sid, {'normal'}):
        raise ValueError('Некоректна діагностична позначка')
    return data


class Connection:
    """Мережевий цикл працює у фоновому потоці Paho; очікуємо CONNACK."""
    def __init__(self, client_id, reconnect_on_failure=True):
        self.ready = threading.Event()
        self.error = None
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id=client_id, protocol=mqtt.MQTTv311,
                                  reconnect_on_failure=reconnect_on_failure)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

    def on_connect(self, client, userdata, flags, reason_code, properties):
        self.error = str(reason_code) if reason_code.is_failure else None
        self.ready.set()

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.ready.clear()

    def start(self, host, port, keepalive=30):
        self.client.connect(host, port, keepalive=keepalive)
        self.client.loop_start()
        if not self.ready.wait(10) or self.error or not self.client.is_connected():
            self.close()
            raise RuntimeError(f'Не вдалося підключитися: {self.error or "тайм-аут"}')

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()


def status_payload(device_id, run_id, state, reason):
    # Час майбутнього обриву невідомий під час реєстрації Will.
    return json.dumps({'schema_version': 1, 'device_id': device_id,
                       'run_id': run_id, 'state': state, 'reason': reason,
                       'timestamp': None if reason == 'unexpected_disconnect' else utc_now()})


def validate_status(topic, payload):
    if len(payload) > 65536:
        raise ValueError('Статус перевищує 64 KiB')
    try:
        data = json.loads(payload.decode('utf-8'), object_pairs_hook=unique_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError('Очікується UTF-8 JSON статусу') from exc
    expected = {'schema_version', 'device_id', 'run_id', 'state', 'reason', 'timestamp'}
    if not isinstance(data, dict) or set(data) != expected:
        raise ValueError('Некоректні поля статусу')
    if type(data['schema_version']) is not int or data['schema_version'] != 1:
        raise ValueError('Невідома версія статусу')
    if not identifier(data['device_id']) or not identifier(data['run_id']):
        raise ValueError('Некоректні ID статусу')
    if topic != f"dt/{data['device_id']}/status":
        raise ValueError('Статус не відповідає топіку')
    allowed = [('online', 'connected'), ('offline', 'normal_shutdown'),
               ('offline', 'publisher_error'), ('offline', 'unexpected_disconnect')]
    if (data['state'], data['reason']) not in allowed:
        raise ValueError('Некоректна пара state/reason')
    if data['reason'] == 'unexpected_disconnect':
        if data['timestamp'] is not None:
            raise ValueError('Для Will timestamp має бути null; час фіксує subscriber')
    else:
        timestamp(data['timestamp'])
    return data
