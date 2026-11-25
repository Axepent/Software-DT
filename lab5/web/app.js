import { createScene } from './scene.js';
import { createHistory } from './history.js';

const element = id => document.getElementById(id);
const levels = { normal: 'Норма', warning: 'Попередження', emergency: 'Аварія', unknown: 'Немає даних' };
const alertLabels = {
  overcurrent: 'Перевищено струм привода',
  overtemperature: 'Перевищено температуру привода',
  voltage_warning: 'Напруга поза робочим діапазоном',
  limit_active: 'Спрацював кінцевий вимикач',
  emergency_input_active: 'Активовано аварійний вхід',
};
const qualityLabels = {
  fresh: 'Актуальне', held: 'Останнє виміряне', latched: 'Зафіксований сигнал',
  stale: 'Застаріле', missing: 'Немає даних', outlier: 'Викид', gap: 'Пропуск',
};
const metrics = [
  { sensor: 'S-07', title: 'Струм', key: 'drive_current', unit: 'А' },
  { sensor: 'S-08', title: 'Температура', key: 'drive_temperature', unit: '°C' },
  { sensor: 'S-09', title: 'Напруга', key: 'supply_voltage', unit: 'В' },
];
const rate = Number(new URLSearchParams(location.search).get('bench'));
const benchmark = [10, 30, 60, 120].includes(rate);
const freshForMs = 2000;
const number = new Intl.NumberFormat('uk-UA', { maximumFractionDigits: 2 });
const format = (value, unit) => Number.isFinite(value) ? number.format(value) + ' ' + unit : '—';
const toggle = value => typeof value === 'boolean' ? (value ? 'активний' : 'вимкнений') : '—';

let model;
let selected = null;
let latest = null;
let socket;
let rendered;
let audio;
let soundEnabled = false;
let activeLevel = 'unknown';
let received = 0;
let measurement;
let frameTimes = [];
let lastRefresh = -Infinity;
let connection = 'Підключення до WebSocket…';

function currentLevel(state) {
  const age = Date.now() - Date.parse(state?.updated_at);
  if (state?.connection_status !== 'online' || !state.broker_connected ||
      !Number.isFinite(age) || age >= freshForMs || !Object.values(state.joint_angles).some(Number.isFinite)) {
    return 'unknown';
  }
  if (state.emergency_input || state.alerts?.includes('emergency_input_active')) return 'emergency';
  return state.alerts?.length ? 'warning' : 'normal';
}

function sound(level) {
  if (!soundEnabled || audio?.state !== 'running') return;
  const frequencies = level === 'emergency' ? [680, 480, 680] : [440, 560];
  frequencies.forEach((frequency, index) => {
    const start = audio.currentTime + index * 0.16;
    const tone = audio.createOscillator();
    const volume = audio.createGain();
    tone.frequency.value = frequency;
    volume.gain.setValueAtTime(0, start);
    volume.gain.linearRampToValueAtTime(0.11, start + 0.02);
    volume.gain.linearRampToValueAtTime(0, start + 0.13);
    tone.connect(volume).connect(audio.destination);
    tone.start(start);
    tone.stop(start + 0.14);
  });
}

element('sound').onclick = async () => {
  try {
    if (!audio) audio = new AudioContext();
    if (!soundEnabled) await audio.resume();
    soundEnabled = !soundEnabled;
    element('sound').setAttribute('aria-pressed', soundEnabled);
    element('sound').textContent = soundEnabled ? 'Вимкнути звук' : 'Увімкнути звук';
  } catch {
    element('sound').textContent = 'Звук недоступний';
  }
};

function makeReading(container, reading, click) {
  const card = document.createElement(click ? 'button' : 'div');
  card.className = click ? 'joint' : 'metric';
  if (click) {
    card.type = 'button';
    card.onclick = click;
    card.setAttribute('aria-pressed', 'false');
  }
  const label = document.createElement('span');
  const value = document.createElement('strong');
  const quality = document.createElement('small');
  label.textContent = reading.title;
  card.append(label, value, quality);
  container.append(card);
  return { ...reading, card, value, quality };
}

async function start() {
  const response = await fetch('/assets/binding.json');
  if (!response.ok) throw new Error('Не вдалося завантажити конфігурацію шарнірів.');
  const bindings = await response.json();
  const sensorSelect = element('sensor');
  const readings = [...metrics, ...bindings.map(binding => ({ ...binding, unit: '°' }))];
  for (const { sensor, title, unit } of readings) {
    sensorSelect.add(new Option(title + ' · ' + unit, sensor));
  }
  createHistory(sensorSelect, element('period'), element('history-chart'), element('history-status'), benchmark);

  const metricCards = metrics.map(item => makeReading(element('readings'), item));
  const jointCards = readings.slice(metrics.length).map(item =>
    makeReading(element('joints'), item, () => select(item.sensor)));
  let alertSignature;

  function qualityOf(state, sensor) {
    const quality = state?.sensor_quality?.[sensor]?.status;
    if (quality && quality !== 'missing' && activeLevel === 'unknown') return qualityLabels.stale;
    return qualityLabels[quality] ?? qualityLabels.missing;
  }

  function select(sensor) {
    selected = sensor;
    for (const { card, sensor: id } of jointCards) card.setAttribute('aria-pressed', id === sensor);
    element('selection').hidden = !sensor;
    if (sensor) {
      const item = jointCards.find(card => card.sensor === sensor);
      element('selection-title').textContent = item.title + ' · ' + sensor;
    }
    model?.focusOn(sensor);
    refresh();
  }

  function refresh() {
    const state = latest;
    element('severity').dataset.level = activeLevel;
    element('severity').textContent = levels[activeLevel];
    element('source').textContent = state
      ? [connection, 'версія ' + state.version, new Date(state.updated_at).toLocaleTimeString('uk-UA')].join(' · ')
      : connection;
    if (state?.device_id) element('device').textContent = state.device_id;
    const alerts = activeLevel === 'unknown' ? [] : (state?.alerts ?? []);
    const signature = alerts.join('|');
    if (signature !== alertSignature) {
      element('alerts').replaceChildren(...alerts.map(alert => {
        const item = document.createElement('li');
        item.textContent = alertLabels[alert] ?? alert;
        return item;
      }));
      alertSignature = signature;
    }
    element('switches').textContent = 'Кінцевий вимикач — ' + toggle(state?.limit_switch)
      + ' · Аварійний вхід — ' + toggle(state?.emergency_input);
    for (const item of metricCards) {
      item.value.textContent = format(state?.[item.key], item.unit);
      item.quality.textContent = qualityOf(state, item.sensor);
    }
    for (const item of jointCards) {
      item.value.textContent = format(state?.joint_angles[item.sensor], item.unit);
      item.quality.textContent = qualityOf(state, item.sensor);
    }
    if (selected) {
      element('selection-value').textContent = format(state?.joint_angles[selected], '°');
      element('selection-quality').textContent = qualityOf(state, selected);
    }
  }

  function frame(now) {
    if (model && (rendered !== latest || currentLevel(latest) !== activeLevel)) {
      activeLevel = currentLevel(latest);
      rendered = latest;
      model.update(latest, activeLevel);
    }
    if (now - lastRefresh >= 125) {
      lastRefresh = now;
      refresh();
    }
    if (!measurement) return;
    frameTimes.push(now);
    if (now - measurement.start < 10000) return;
    const seconds = (now - frameTimes[0]) / 1000;
    const intervals = frameTimes.slice(1).map((time, index) => time - frameTimes[index]).sort((a, b) => a - b);
    const result = {
      targetHz: rate,
      receivedHz: Number(((received - measurement.received) / ((now - measurement.start) / 1000)).toFixed(2)),
      fps: Number(((frameTimes.length - 1) / seconds).toFixed(2)),
      p95Ms: Number(intervals[Math.ceil(intervals.length * 0.95) - 1].toFixed(2)),
    };
    const status = element('benchmark-status');
    status.dataset.result = JSON.stringify(result);
    status.textContent = result.receivedHz + ' оновл./с · ' + result.fps + ' FPS · P95 ' + result.p95Ms + ' мс';
    measurement = null;
    element('benchmark-start').disabled = false;
  }

  element('close-selection').onclick = () => select(null);
  element('reset').onclick = () => { select(null); model?.reset(); };
  if (benchmark) {
    element('benchmark').hidden = false;
    element('benchmark-status').textContent = 'Вхідний потік ' + rate + ' Гц · замір 10 секунд';
    element('benchmark-start').disabled = true;
    element('benchmark-start').onclick = () => {
      frameTimes = [];
      measurement = { start: performance.now(), received };
      element('benchmark-status').textContent = 'Триває замір…';
      delete element('benchmark-status').dataset.result;
      element('benchmark-start').disabled = true;
    };
  }

  try {
    model = await createScene(element('viewport'), bindings, select, frame);
    element('scene-message').hidden = true;
    element('benchmark-start').disabled = false;
  } catch (error) {
    element('scene-message').textContent = '3D-сцена недоступна. ' + error.message;
    setInterval(() => frame(performance.now()), 125);
  }

  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  let previous = 'unknown';
  function connect() {
    socket = new WebSocket(protocol + '//' + location.host + (benchmark ? '/ws/benchmark/' + rate : '/ws/state'));
    socket.onopen = () => {
      connection = benchmark ? 'Синтетичний потік · ' + rate + ' Гц' : 'MQTT · очікування стану';
      refresh();
    };
    socket.onmessage = event => {
      const state = JSON.parse(event.data);
      received += 1;
      const next = currentLevel(state);
      if (next !== previous && (next === 'warning' || next === 'emergency')) sound(next);
      previous = next;
      latest = state;
      if (!benchmark) {
        connection = !state ? 'MQTT · немає знімка стану' :
          state.connection_status === 'online' ? 'MQTT · поточний стан' : 'MQTT · джерело офлайн';
      }
    };
    socket.onclose = () => {
      previous = 'unknown';
      latest = null;
      connection = 'WebSocket · повторне підключення';
      setTimeout(connect, 1500);
    };
  }
  refresh();
  connect();
}

start().catch(error => { element('scene-message').textContent = error.message; });
