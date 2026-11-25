const periods = { '5m': 300000, '15m': 900000, '1h': 3600000, '24h': 86400000 };
const shortTime = new Intl.DateTimeFormat('uk-UA', { hour: '2-digit', minute: '2-digit' });
const dayTime = new Intl.DateTimeFormat('uk-UA', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
const axisNumber = new Intl.NumberFormat('uk-UA', { maximumFractionDigits: 2 });

export function createHistory(sensor, period, canvas, status, benchmark) {
  let abort;
  let history;
  let rangeEnd;
  const ctx = canvas.getContext('2d');

  function draw() {
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    const scale = devicePixelRatio || 1;
    canvas.width = width * scale;
    canvas.height = height * scale;
    ctx.scale(scale, scale);
    ctx.fillStyle = '#101824';
    ctx.fillRect(0, 0, width, height);
    if (!history?.points.length) return;

    const left = 48, right = width - 13, top = 13, bottom = height - 23;
    const start = rangeEnd - periods[period.value];
    const points = history.points.map(([time, value]) => [Date.parse(time), value]);
    const values = points.map(point => point[1]);
    const minimum = Math.min(...values);
    const maximum = Math.max(...values);
    const margin = (maximum - minimum) * 0.12 || Math.max(0.2, Math.abs(minimum) * 0.05);
    const low = minimum - margin;
    const high = maximum + margin;
    const x = time => left + (right - left) * (time - start) / (rangeEnd - start);
    const y = value => bottom - (bottom - top) * (value - low) / (high - low);

    ctx.font = '11px system-ui, sans-serif';
    ctx.fillStyle = '#a2b2c8';
    ctx.strokeStyle = '#2a3748';
    ctx.textAlign = 'right';
    for (let tick = 0; tick <= 3; tick++) {
      const value = low + (high - low) * tick / 3;
      const row = y(value);
      ctx.fillText(axisNumber.format(value), left - 5, row + 4);
      ctx.beginPath();
      ctx.moveTo(left, row);
      ctx.lineTo(right, row);
      ctx.stroke();
    }
    const timeFormat = period.value === '24h' ? dayTime : shortTime;
    ctx.textAlign = 'left';
    ctx.fillText(timeFormat.format(start), left, bottom + 16);
    ctx.textAlign = 'right';
    ctx.fillText(timeFormat.format(rangeEnd), right, bottom + 16);

    ctx.save();
    ctx.beginPath();
    ctx.rect(left, top, right - left, bottom - top);
    ctx.clip();
    ctx.strokeStyle = '#80b5ff';
    ctx.lineWidth = 1.6;
    ctx.fillStyle = '#80b5ff';
    for (const [index, [time, value]] of points.entries()) {
      const previous = points[index - 1];
      if (previous && time - previous[0] <= history.step_ms * 1.5) {
        ctx.beginPath();
        ctx.moveTo(x(previous[0]), y(previous[1]));
        ctx.lineTo(x(time), y(value));
        ctx.stroke();
      } else {
        ctx.fillRect(x(time) - 1, y(value) - 1, 3, 3);
      }
    }
    ctx.restore();
  }

  async function load(clear = false) {
    abort?.abort();
    abort = new AbortController();
    if (clear) {
      history = null;
      status.textContent = 'Завантаження історії…';
      draw();
    }
    try {
      const params = new URLSearchParams({ sensor: sensor.value, period: period.value });
      const response = await fetch('/api/history?' + params, { signal: abort.signal });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error);
      history = result;
      rangeEnd = Date.now();
      status.textContent = result.points.length
        ? result.points.length + ' точок · середні значення за ' + result.step_ms / 1000 + ' с'
        : 'За вибраний період даних немає.';
      draw();
    } catch (error) {
      if (error.name === 'AbortError') return;
      history = null;
      status.textContent = error.message;
      draw();
    }
  }

  new ResizeObserver(draw).observe(canvas);
  if (benchmark) {
    status.textContent = 'Для заміру історія вимкнена.';
    sensor.disabled = period.disabled = true;
    return;
  }
  sensor.onchange = period.onchange = () => load(true);
  load(true);
  setInterval(() => load(), 10000);
}
