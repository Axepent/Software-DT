"""Окремий підписник: доказ доставки повідомлень про стан."""
import argparse
import json
import os
import time
from pathlib import Path
import paho.mqtt.client as mqtt


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=1883)
    p.add_argument('--device-id', default='mannequin-01')
    p.add_argument('--duration', type=float, default=0)
    p.add_argument('--output', type=Path, default=Path('results/notifications.jsonl'))
    args = p.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if os.getenv('MQTT_USERNAME'):
        client.username_pw_set(os.environ['MQTT_USERNAME'], os.getenv('MQTT_PASSWORD'))
    with args.output.open('w', encoding='utf-8') as out:
        def connected(c, u, flags, code, props):
            c.subscribe(f'dt/{args.device_id}/state', qos=1)
        def received(c, u, msg):
            if msg.topic != f'dt/{args.device_id}/state':
                return
            state = json.loads(msg.payload)
            out.write(json.dumps(dict(received_at=time.time(), retained=bool(msg.retain), state=state))+'\n')
            out.flush()
            print(f"STATE {state['version']} | {state['reason']} | {state['alerts']}", flush=True)
        client.on_connect, client.on_message = connected, received
        client.connect(args.host, args.port, 5)
        client.loop_start()
        start = time.monotonic()
        try:
            while not args.duration or time.monotonic()-start < args.duration:
                time.sleep(.1)
        except KeyboardInterrupt:
            pass
        finally:
            client.disconnect()
            client.loop_stop()


if __name__ == '__main__':
    main()
