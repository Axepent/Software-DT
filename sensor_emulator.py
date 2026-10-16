"""
duration — модельні секунди; speed=1 — реальний темп, 60 — прискорення
у 60 разів, 0 — без очікування. Частоти визначено за МОДЕЛЬНИМ часом.
З однаковими seed і start-time результат відтворюється незалежно від speed.
CSV містить один рядок на відлік. 

Межі, пороги та параметри динаміки є припущеннями навчального стенда,
а не характеристиками реальних датчиків чи фізичною моделлю привода.
Кути — шість скалярних осей. D-01 відповідає плечовій осі S-01.
"""

import argparse
import csv
import math
import random
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Union


@dataclass(frozen=True)
class Sensor:
    name: str
    unit: str
    minimum: float
    maximum: float
    period_ticks: int  # Один такт = 0,01 с; 0 означає подієвий сигнал.
    noise: float


SENSORS = {
    'S-01': Sensor('Кут згинання плеча', 'deg', -90, 90, 10, 0.08),
    'S-02': Sensor('Кут згинання ліктя', 'deg', 0, 135, 10, 0.08),
    'S-03': Sensor('Кут згинання кульшового шарніра', 'deg', -60, 90, 10, 0.08),
    'S-04': Sensor('Кут згинання коліна', 'deg', 0, 135, 10, 0.08),
    'S-05': Sensor('Кут повороту голови', 'deg', -60, 60, 10, 0.05),
    'S-06': Sensor('Кут нахилу тулуба', 'deg', -45, 45, 10, 0.05),
    'S-07': Sensor('Струм привода D-01', 'A', 0, 5, 1, 0.02),
    'S-08': Sensor('Температура привода D-01', 'degC', -10, 100, 100, 0.05),
    'S-09': Sensor('Напруга живлення', 'V', 0, 15, 10, 0.015),
    'S-10': Sensor('Кінцевий вимикач', '', 0, 1, 0, 0),
    'S-11': Sensor('Вхід аварійної зупинки', '', 0, 1, 0, 0),
}

# Пороги попереджень відокремлені від меж допустимих даних.
CURRENT_WARNING = 3.0
TEMPERATURE_WARNING = 70.0
VOLTAGE_LOW = 10.8
VOLTAGE_HIGH = 13.2
STEP = 0.01
CYCLE = 120.0
JOINT_IDS = tuple(SENSORS)[:6]
# Фіксовані опорні пози з плавними переходами і паузами між ними.
POSES = (
    (0, 20, 0, 10, 0, 0),
    (45, 100, 30, 70, -30, 15),
    (-35, 40, -20, 30, 35, -20),
    (70, 120, 65, 100, -45, 30),
)


@dataclass(frozen=True)
class Sample:
    device_id: str
    sample_id: int
    sensor_id: str
    timestamp: str  
    sim_time_s: float
    value: Union[float, bool]
    unit: str
    condition: str  


def clamp(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def parse_start(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('start-time має містити часовий пояс, наприклад Z')
    return result.astimezone(timezone.utc)


class SensorEmulator:
    """Генератор показань; не залежить від CSV, консолі чи MQTT."""

    def __init__(self, seed: int = 42, device_id: str = 'mannequin-01',
                 start_time: Union[datetime, None] = None, anomalies: bool = True):
        self.rng = random.Random(seed)
        self.device_id = device_id
        self.start_time = start_time or datetime.now(timezone.utc)
        if self.start_time.tzinfo is None:
            raise ValueError('start_time має містити часовий пояс')
        self.start_time = self.start_time.astimezone(timezone.utc)
        self.anomalies = anomalies

    def samples(self, duration: float) -> Iterator[Sample]:
        """Один прогін [0, duration); повторний виклик створює новий прогін.

        Екземпляр призначений для одного прогону: RNG між викликами не скидається.
        Подієві канали: початковий стан і лише наступні зміни стану.
        """
        if not math.isfinite(duration) or duration <= 0:
            raise ValueError('duration має бути скінченним додатним числом')
        angles = list(POSES[0])
        temperature = 25.0
        previous_events = {}
        sequence = 0
        for tick in range(math.ceil(duration / STEP)):
            t = round(tick * STEP, 2)
            if t >= duration:
                break
            phase = t % CYCLE
            overload = self.anomalies and 25 <= phase < 35
            at_limit = self.anomalies and 42 <= phase < 47
            voltage_drop = self.anomalies and 55 <= phase < 63
            cooling_fault = self.anomalies and 65 <= phase < 100
            emergency_input = self.anomalies and 75 <= phase < 79
            # Вхід відпущено на 79 с, але рух заблоковано до явного
            # модельованого підтвердження відновлення на 90 с.
            emergency_latched = self.anomalies and 75 <= phase < 90

            segment = int(t // 15)
            a, b = POSES[segment % len(POSES)], POSES[(segment + 1) % len(POSES)]
            u = min((t % 15) / 6.0, 1.0)  # 6 с перехід, потім 9 с утримання.
            smooth = u * u * (3 - 2 * u)
            targets = [x + (y - x) * smooth for x, y in zip(a, b)]
            if at_limit:
                targets[0] = 90.0
            previous_shoulder = angles[0]
            if not emergency_latched:
                for i, target in enumerate(targets):
                    # Обмежена швидкість: 25 градусів за секунду.
                    angles[i] += clamp(target - angles[i], -25 * STEP, 25 * STEP)
            shoulder_speed = abs(angles[0] - previous_shoulder) / STEP
            limit_switch = angles[0] >= 89.9

            # Струм реагує на рух і утримання навантаженої плечової осі.
            current = (0.12 if emergency_latched else
                       0.35 + 0.025 * shoulder_speed + 0.2 * abs(angles[0]) / 90)
            if overload and not emergency_latched:
                current += 3.2

            utc = self.start_time + timedelta(seconds=t)
            seconds_of_day = (utc.hour * 3600 + utc.minute * 60 + utc.second
                              + utc.microsecond / 1_000_000)
            # Фон 20..26 °C, максимум о 15:00 UTC; окремого датчика
            # довкілля не додаємо. Часову фазу можна змінити у формулі
            ambient = 23 + 3 * math.cos(2 * math.pi * (seconds_of_day - 54000) / 86400)
            # Повільний тренд прогрівання електроніки за перші хвилини
            warmup = 2 * (1 - math.exp(-t / 600))
            thermal_target = ambient + warmup + 3 * current * current
            # Умовна несправність охолодження/зовнішнє нагрівання.
            # Скорочена стала часу обрана для навчальної демонстрації
            if cooling_fault:
                thermal_target += 65
            tau = 15.0 if cooling_fault else 60.0
            temperature += (thermal_target - temperature) * (1 - math.exp(-STEP / tau))
            # Скiнченна тривалість просадки з плавними фронтами
            sag = 0.0
            if voltage_drop:
                sag = 2.0 * min((phase - 55) / 0.5, (63 - phase) / 0.5, 1.0)
            voltage = 12 - 0.12 * current - sag

            values = dict(zip(JOINT_IDS, angles))
            values.update({'S-07': current, 'S-08': temperature, 'S-09': voltage,
                           'S-10': limit_switch, 'S-11': emergency_input})
            for sensor_id, sensor in SENSORS.items():
                value = values[sensor_id]
                if sensor.period_ticks:
                    if tick % sensor.period_ticks:
                        continue
                    value = round(clamp(value + self.rng.gauss(0, sensor.noise),
                                        sensor.minimum, sensor.maximum), 4)
                else:
                    if sensor_id in previous_events and previous_events[sensor_id] == value:
                        continue
                    previous_events[sensor_id] = value

                condition = 'normal'
                if sensor_id == 'S-07' and value > CURRENT_WARNING:
                    condition = 'overcurrent'
                elif sensor_id == 'S-08' and value > TEMPERATURE_WARNING:
                    condition = 'overtemperature'
                elif sensor_id == 'S-09' and not VOLTAGE_LOW <= value <= VOLTAGE_HIGH:
                    condition = 'voltage_warning'
                elif sensor_id == 'S-10' and value:
                    condition = 'limit_active'
                elif sensor_id == 'S-11' and value:
                    condition = 'emergency_input_active'
                sequence += 1
                yield Sample(self.device_id, sequence, sensor_id,
                             utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                             t, value, sensor.unit, condition)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--duration', type=float, default=120, help='Модельна тривалість, с')
    parser.add_argument('--speed', type=float, default=1, help='Прискорення; 0 = без очікування')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--device-id', default='mannequin-01')
    parser.add_argument('--start-time', type=parse_start, default=None,
                        help='Початок модельного часу, наприклад 2026-10-16T08:00:00Z')
    parser.add_argument('--output', type=Path, default=Path('telemetry.csv'))
    parser.add_argument('--no-anomalies', action='store_true')
    parser.add_argument('--quiet', action='store_true', help='Лише підсумок, без поточних показань')
    args = parser.parse_args()
    if not math.isfinite(args.duration) or args.duration <= 0:
        parser.error('duration має бути скінченним додатним числом')
    if not math.isfinite(args.speed) or args.speed < 0:
        parser.error('speed має бути скінченним невід’ємним числом')
    emulator = SensorEmulator(args.seed, args.device_id, args.start_time, not args.no_anomalies)
    counts, warnings = Counter(), Counter()
    latest = {}
    last_second = -1
    started = time.perf_counter()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('w', newline='', encoding='utf-8-sig') as output:
        writer = csv.DictWriter(output, fieldnames=list(Sample.__dataclass_fields__))
        writer.writeheader()
        try:
            for sample in emulator.samples(args.duration):
                if args.speed:
                    wait = started + sample.sim_time_s / args.speed - time.perf_counter()
                    if wait > 0:
                        time.sleep(wait)
                writer.writerow(asdict(sample))
                counts[sample.sensor_id] += 1
                if sample.condition != 'normal':
                    warnings[sample.condition] += 1
                latest[sample.sensor_id] = sample.value
                second = int(sample.sim_time_s)
                # S-09 іде після всіх числових каналів поточного такту.
                if not args.quiet and sample.sensor_id == 'S-09' and second != last_second:
                    last_second = second
                    print(f"t={second:5d}s | плече={latest['S-01']:7.2f} deg | "
                          f"I={latest['S-07']:.2f} A | T={latest['S-08']:.2f} C | "
                          f"U={latest['S-09']:.2f} V")
        except KeyboardInterrupt:
            print('\nПрогін зупинено; сформовані відліки збережено.')
    print(f'Файл: {args.output.resolve()}')
    print(f'Відліків: {sum(counts.values())}; за сигналами: {dict(counts)}')
    print(f'Відліків із попередженнями (не кількість окремих аварій): {dict(warnings)}')
    print('Часові мітки позначають модельний час; прискорений прогін не вимірює затримку MQTT.')


if __name__ == '__main__':
    main()
