"""Latest-state MQTT -> HTTP adapter. History remains in Lab 3 InfluxDB."""
import argparse
import json
import os
import queue
import time
from pathlib import Path
import requests
import paho.mqtt.client as mqtt
from .api import API
from .model import project

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--host', default='127.0.0.1'); p.add_argument('--port', type=int, default=1883)
    p.add_argument('--interval', type=float, default=1.0)
    p.add_argument('--output', type=Path, default=Path('results/lab4/adapter.jsonl'))
    a = p.parse_args()
    if a.interval < 0: p.error('interval must be nonnegative')
    pending = queue.Queue(maxsize=1)
    api = API()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='lab4-ditto-adapter')
    if os.getenv('MQTT_USERNAME'):
        client.username_pw_set(os.environ['MQTT_USERNAME'], os.getenv('MQTT_PASSWORD'))
    def connected(c,u,f,rc,props):
        if not rc.is_failure: c.subscribe('dt/mannequin-01/state', qos=1)
    def message(c,u,msg):
        try:
            s = json.loads(msg.payload); project(s)
            if pending.full(): pending.get_nowait()
            pending.put_nowait(s)
        except (ValueError, KeyError, TypeError) as e: print('INVALID STATE', str(e), flush=True)
    client.on_connect = connected; client.on_message = message
    client.reconnect_delay_set(1,10)
    client.connect(a.host,a.port); client.loop_start()
    a.output.parent.mkdir(parents=True, exist_ok=True)
    current = None; last = None
    try:
        with a.output.open('a', encoding='utf8') as log:
            while True:
                try: current = pending.get(timeout=1) if current is None else pending.get_nowait()
                except queue.Empty: pass
                if current is None: continue
                key = (current['run_id'], current['version'])
                if last and key[0] == last[0] and key[1] <= last[1]:
                    current = None; continue
                started = time.monotonic()
                try:
                    for id, features in project(current).items(): api.update(id, features)
                except requests.RequestException as e:
                    print('RETRY (partial graph possible)', str(e), flush=True)
                    time.sleep(2); continue
                record = dict(run_id=key[0], version=key[1], updatedThings=10,
                              elapsed_ms=round((time.monotonic()-started)*1000,2))
                log.write(json.dumps(record)+'\n'); log.flush(); print(record, flush=True)
                last=key; current=None
                time.sleep(a.interval)
    except KeyboardInterrupt: pass
    finally: client.disconnect(); client.loop_stop()
if __name__ == '__main__': main()
