"""Журнали та стійка черга повторного запису в InfluxDB 2.x HTTP API."""
import json
import sqlite3
import threading
import time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import URLError
from twin_state import seconds


def tag(value):
    return str(value).replace('\\', '\\\\').replace(' ', '\\ ').replace(',', '\\,').replace('=', '\\=')


def field(value):
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, int):
        return str(value) + 'i'
    if isinstance(value, float):
        return repr(value)
    return json.dumps(str(value), ensure_ascii=False)


def line(measurement, tags, fields, timestamp):
    # Усі числові показання сенсорів записуємо як float: без конфлікту типів
    # InfluxDB при змішаних вхідних int/float. sample_id залишається int.
    return (measurement + ',' + ','.join(f'{k}={tag(v)}' for k, v in tags.items())
            + ' ' + ','.join(f'{k}={field(v)}' for k, v in fields.items() if v is not None)
            + ' ' + str(timestamp))


class HistoryStore:
    def __init__(self, directory, url=None, org='dt-lab', bucket='mannequin', token=None):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.files = {name: (self.directory / (name + '.jsonl')).open('a', encoding='utf-8')
                      for name in ('telemetry', 'states', 'errors', 'notifications')}
        self.url, self.token = url, token
        self.stop = threading.Event()
        self.db = sqlite3.connect(self.directory / 'outbox.sqlite3')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY, line TEXT NOT NULL)')
        self.db.commit()
        self.endpoint = (url.rstrip('/') + '/api/v2/write?' + urlencode(
            dict(org=org, bucket=bucket, precision='ns'))) if url else None
        self.last_error = None
        self.written = 0
        self.thread = None
        self.sequence_ns = time.time_ns()
        if url:
            if not token:
                raise ValueError('Потрібен INFLUX_TOKEN')
            self.thread = threading.Thread(target=self._worker, daemon=True)
            self.thread.start()

    def journal(self, name, value):
        self.files[name].write(json.dumps(value, ensure_ascii=False, allow_nan=False) + '\n')

    def _enqueue(self, value):
        # SQLite outbox створюється і в локальному режимі: історію можна
        # дослати до InfluxDB наступним запуском з тим самим --output.
        self.db.execute('INSERT INTO outbox(line) VALUES (?)', (value,))

    def raw(self, record):
        self.journal('telemetry', record)
        data = record['data']
        tags = {k: data[k] for k in ('device_id', 'run_id', 'sensor_id')}
        fields = dict(sample_id=data['sample_id'], condition=data['condition'],
                      outcome=record['outcome'], unit=data['unit'],
                      generated_at=data['timestamp'], received_at=record['received_at'])
        if isinstance(data['value'], bool):
            fields['discrete_value'] = data['value']
        else:
            fields['value'] = float(data['value'])
        # Час точки = час спостереження; generated_at зберігається окремо.
        # Монотонний ns запобігає перезапису різних відліків з однією міткою.
        self.sequence_ns = max(self.sequence_ns + 1, time.time_ns())
        self._enqueue(line('telemetry', tags, fields, self.sequence_ns))

    def state(self, state):
        self.journal('states', state)
        fields = {k: float(v) for k, v in state['joint_angles'].items() if v is not None}
        for key in ('drive_current', 'drive_temperature', 'supply_voltage',
                    'current_mean_100ms', 'current_max_100ms', 'current_mean_1s', 'current_max_1s'):
            if state[key] is not None:
                fields[key] = float(state[key])
        for key in ('version', 'updated_at', 'event_time', 'reason', 'connection_status',
                    'broker_connected', 'limit_switch', 'emergency_input'):
            fields[key] = state[key]
        fields['alerts_json'] = json.dumps(state['alerts'])
        fields['quality_json'] = json.dumps(state['sensor_quality'])
        self.sequence_ns = max(self.sequence_ns + 1, time.time_ns())
        self._enqueue(line('twin_state', dict(device_id=state['device_id'], run_id=state['run_id']),
                           fields, self.sequence_ns))

    def flush(self):
        self.db.commit()
        for f in self.files.values():
            f.flush()

    def _worker(self):
        db = sqlite3.connect(self.directory / 'outbox.sqlite3', timeout=10)
        try:
            while not self.stop.is_set():
                rows = db.execute('SELECT id, line FROM outbox ORDER BY id LIMIT 1000').fetchall()
                if not rows:
                    self.stop.wait(.2)
                    continue
                request = Request(self.endpoint, data='\n'.join(r[1] for r in rows).encode(),
                                  headers={'Authorization': 'Token ' + self.token,
                                           'Content-Type': 'text/plain; charset=utf-8'}, method='POST')
                try:
                    with urlopen(request, timeout=5) as response:
                        if response.status != 204:
                            raise OSError(f'InfluxDB HTTP {response.status}')
                    db.executemany('DELETE FROM outbox WHERE id=?', [(r[0],) for r in rows])
                    db.commit()
                    self.written += len(rows)
                    self.last_error = None
                except (OSError, URLError) as exc:
                    # Непідтверджені записи не видаляються. Повторний POST
                    # містить ті самі series/timestamps, тобто є ідемпотентним.
                    self.last_error = str(exc)
                    self.stop.wait(1)
        finally:
            db.close()

    def pending(self):
        return self.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0]

    def close(self):
        self.flush()
        deadline = time.monotonic() + 8
        while self.thread and self.pending() and time.monotonic() < deadline:
            time.sleep(.1)
        self.stop.set()
        if self.thread:
            self.thread.join(7)
        result = dict(influx_enabled=bool(self.url), influx_written=self.written,
                      outbox_pending=self.pending(), influx_error=self.last_error)
        self.db.close()
        for f in self.files.values():
            f.close()
        return result
