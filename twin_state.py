"""Модель стану та потокове ядро. Не залежить від MQTT або бази даних."""
import copy
import heapq
import math
from collections import Counter, OrderedDict, deque
from datetime import datetime, timezone

from sensor_emulator import (SENSORS, JOINT_IDS, CURRENT_WARNING,
                             TEMPERATURE_WARNING, VOLTAGE_LOW, VOLTAGE_HIGH)


def iso(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def seconds(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


class TwinPipeline:
    """Один манекен; виклики лише з одного потоку.

    Event-time сітка 100 мс і допустиме очікування 100 мс. Пізні дані
    архівуються, але не переписують вже завершені часові інтервали.
    """
    def __init__(self, device_id='mannequin-01', period=0.1, reorder=0.1,
                 angle_threshold=1.0, on_state=None, on_raw=None, on_metric=None,
                 capacity=50000, clock=None):
        if period <= 0 or reorder < 0 or angle_threshold <= 0 or capacity <= 0:
            raise ValueError('Некоректні параметри конвеєра')
        self.clock = clock
        self.device_id, self.period, self.reorder = device_id, period, reorder
        self.threshold, self.capacity = angle_threshold, capacity
        self.on_state = on_state or (lambda state: None)
        self.on_raw = on_raw or (lambda record: None)
        self.on_metric = on_metric or (lambda record: None)
        self.counts = Counter()
        self.heap, self.serial = [], 0
        self.seen = OrderedDict()
        self.retired = set()
        self.run_id = None
        self.last = {}
        self.quality_override = {}
        self.current_window = deque()
        self.current_bin = []
        self.pending = []
        self.version = 0
        self.next_tick = None
        self.closed_until = float('-inf')
        self.connection = 'unknown'
        self.last_emitted = None
        self.last_quality = None
        self.broker_connected = True
        self.last_source_time = float('-inf')

    def _new_run(self, run_id):
        if self.run_id is not None:
            self.retired.add(self.run_id)
        self.run_id = run_id
        self.last.clear()
        self.quality_override.clear()
        self.current_window.clear()
        self.current_bin.clear()
        self.pending.clear()
        # Версія стану монотонна в межах процесу, навіть після зміни run_id.
        self.last_emitted = None

    def ingest(self, data, received_at):
        self.counts['received'] += 1
        if data['device_id'] != self.device_id:
            self.counts['other_device'] += 1
            return
        key = (data['run_id'], data['sample_id'])
        if key in self.seen:
            self.counts['duplicate'] += 1
            return
        t = seconds(data['timestamp'])
        if t > received_at + 2:
            self.counts['future'] += 1
            self.on_raw(dict(data=data, received_at=iso(received_at), outcome='future'))
            return
        if len(self.heap) >= self.capacity:
            self.counts['overflow'] += 1
            self.on_raw(dict(data=data, received_at=iso(received_at), outcome='overflow'))
            return
        self.seen[key] = None
        if len(self.seen) > 100000:
            self.seen.popitem(last=False)
        self.serial += 1
        heapq.heappush(self.heap, (t, self.serial, data, received_at))
        self.counts['unique'] += 1
        self.counts['buffer_peak'] = max(self.counts['buffer_peak'], len(self.heap))
        if self.next_tick is None:
            # Не створюємо мільйони порожніх станів для старого backlog.
            self.next_tick = math.floor(received_at / self.period) * self.period

    def status(self, data, now, retained=False):
        if data['device_id'] != self.device_id or data['run_id'] in self.retired:
            return
        if data['run_id'] != self.run_id:
            # Старий retained offline не повинен створювати новий сеанс.
            if data['state'] != 'online':
                return
            if self.run_id is not None and data['timestamp'] is not None:
                if seconds(data['timestamp']) < self.last_source_time:
                    return
            self._new_run(data['run_id'])
        self.connection = data['state']
        self.counts['status'] += 1
        self.emit(now, now, 'connection')

    def broker_status(self, connected, now):
        self.broker_connected = connected
        if self.run_id:
            self.emit(now, now, 'broker_connection')

    def advance(self, now):
        watermark = now - self.reorder
        # Історичні дані з backlog обробляємо один раз, без відтворення старих
        # станів у реальному часі. Порядок у heap визначається timestamp.
        if self.next_tick is None:
            return
        while self.next_tick <= watermark + 1e-7:
            cutoff = self.next_tick
            self._drain(cutoff, now)
            if self.run_id:
                self.emit(now, cutoff, 'periodic')
            self.closed_until = cutoff
            self.current_bin.clear()
            self.next_tick = round(self.next_tick + self.period, 6)
        # Події та порогові зміни між тактами не чекають наступного такту.
        self._drain(watermark, now)

    def _drain(self, cutoff, now):
        while self.heap and self.heap[0][0] <= cutoff + 1e-7:
            t, _, data, received = heapq.heappop(self.heap)
            self._apply(data, t, received, now)

    def _apply(self, data, t, received, now):
        sid, value, run = data['sensor_id'], data['value'], data['run_id']
        outcome = 'applied'
        if run in self.retired:
            outcome = 'old_run'
        elif run != self.run_id:
            if t < self.last_source_time:
                outcome = 'old_run'
            else:
                self._new_run(run)
        previous = self.last.get(sid)
        if outcome == 'applied' and (t <= self.closed_until or (previous and t <= previous['t'])):
            outcome = 'late'
        if outcome == 'applied' and sid in JOINT_IDS and previous:
            # Фізично правдоподібна швидкість + запас на шум. Пороги аварій
            # струму/температури не є критерієм відкидання показань.
            if abs(value - previous['value']) > 90 * (t - previous['t']) + 1:
                outcome = 'outlier'
                self.quality_override[sid] = 'outlier_held'
        self.counts[outcome] += 1
        self.on_raw(dict(data=data, received_at=iso(received), outcome=outcome))
        if outcome != 'applied':
            return
        self.last[sid] = dict(t=t, value=value, sample_id=data['sample_id'])
        self.quality_override.pop(sid, None)
        self.last_source_time = max(self.last_source_time, t)
        self.pending.append(dict(device_id=self.device_id, run_id=run,
                                 sample_id=data['sample_id'], sensor_id=sid,
                                 generated_at=data['timestamp'], received_at=iso(received)))
        if sid == 'S-07':
            self.current_window.append((t, value))
            self.current_bin.append(value)
        if self.connection == 'unknown':
            self.connection = 'online'
        if sid in ('S-10', 'S-11') and (previous is None or previous['value'] != value):
            self.emit(now, t, 'event')
        elif sid in JOINT_IDS and self.last_emitted:
            old = self.last_emitted['joint_angles'].get(sid)
            if old is not None and abs(value - old) >= self.threshold:
                self.emit(now, t, 'threshold')
        if self.last_emitted and self.alerts() != self.last_emitted['alerts']:
            self.emit(now, t, 'alert')

    def alerts(self):
        def val(sid):
            return self.last.get(sid, {}).get('value')
        result = []
        for sid, name, predicate in (
            ('S-07', 'overcurrent', lambda x: x > CURRENT_WARNING),
            ('S-08', 'overtemperature', lambda x: x > TEMPERATURE_WARNING),
            ('S-09', 'voltage_warning', lambda x: not VOLTAGE_LOW <= x <= VOLTAGE_HIGH),
            ('S-10', 'limit_active', bool),
            ('S-11', 'emergency_input_active', bool)):
            v = val(sid)
            if v is not None and predicate(v):
                result.append(name)
        return result

    def emit(self, now, event_time, reason):
        if self.clock is not None:
            now = self.clock()
        quality = {}
        for sid, sensor in SENSORS.items():
            item = self.last.get(sid)
            if item is None:
                quality[sid] = dict(status='missing', measured_at=None, age_s=None)
                continue
            age = max(0, now - item['t'])
            ttl = max(0.5, sensor.period_ticks * 0.01 * 3)
            if not sensor.period_ticks:
                status = ('latched' if self.connection == 'online' and self.broker_connected
                          else 'stale')
            else:
                status = 'stale' if age > ttl else self.quality_override.get(sid, 'fresh')
                if status == 'fresh' and age > sensor.period_ticks * .01 + self.reorder + .02:
                    status = 'held'
            quality[sid] = dict(status=status, measured_at=iso(item['t']), age_s=round(age, 6))
        while self.current_window and self.current_window[0][0] < event_time - 1:
            self.current_window.popleft()
        currents = [v for t, v in self.current_window if t <= event_time]
        values = {sid: item['value'] for sid, item in self.last.items()}
        self.version += 1
        state = dict(device_id=self.device_id, run_id=self.run_id, version=self.version,
                     updated_at=iso(now), event_time=iso(event_time), reason=reason,
                     connection_status=self.connection, broker_connected=self.broker_connected,
                     joint_angles={sid: values.get(sid) for sid in JOINT_IDS},
                     drive_current=values.get('S-07'), drive_temperature=values.get('S-08'),
                     supply_voltage=values.get('S-09'), limit_switch=values.get('S-10'),
                     emergency_input=values.get('S-11'),
                     current_mean_100ms=(sum(self.current_bin)/len(self.current_bin) if self.current_bin else None),
                     current_max_100ms=(max(self.current_bin) if self.current_bin else None),
                     current_mean_1s=(sum(currents)/len(currents) if currents else None),
                     current_max_1s=(max(currents) if currents else None),
                     alerts=self.alerts(), sensor_quality=quality)
        # Метрика лише для нових вимірювань, уперше представлених у стані.
        for item in self.pending:
            latency = (now - seconds(item['generated_at'])) * 1000
            representation = ('aggregate' if item['sensor_id'] == 'S-07' else
                              'latest' if self.last[item['sensor_id']]['sample_id'] == item['sample_id']
                              else 'superseded')
            self.on_metric(dict(**item, updated_at=iso(now), state_version=self.version,
                                latency_ms=round(latency, 6), clock_valid=latency >= 0,
                                representation=representation))
        self.pending.clear()
        self.on_state(copy.deepcopy(state))
        self.last_emitted = state
        self.counts['states'] += 1
        self.counts['reason_' + reason] += 1
