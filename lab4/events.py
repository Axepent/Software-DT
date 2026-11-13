"""Consume Ditto WebSocket events; react to newly activated pipeline alerts."""
import base64
import json
import time
from pathlib import Path
import websocket
from .api import API
from .model import ROOT, NS

def main():
    api=API(); known=set(); output=Path('results/lab4/events.jsonl')
    output.parent.mkdir(parents=True,exist_ok=True)
    user,password=api.session.auth
    auth=base64.b64encode(f'{user}:{password}'.encode()).decode()
    url=api.url.replace('http://','ws://',1).replace('https://','wss://',1)+'/ws/2'
    try:
        with output.open('a',encoding='utf8') as log:
            while True:
                ws=None
                try:
                    ws=websocket.create_connection(url,header=['Authorization: Basic '+auth],timeout=30)
                    ws.send('START-SEND-EVENTS?namespaces='+NS)
                    print('LISTENING',url,flush=True)
                    while True:
                        try: raw=ws.recv()
                        except websocket.WebSocketTimeoutException:
                            ws.ping(); continue
                        if not raw: raise ConnectionError('WebSocket closed')
                        if raw.startswith('START-SEND-EVENTS:'): print(raw,flush=True); continue
                        event=json.loads(raw)
                        if not event.get('topic','').startswith(NS+'/mannequin-01/things/twin/events/'): continue
                        # Use this event snapshot; no HTTP read racing a later update.
                        value=event.get('value',{})
                        if event.get('path') == '/features':
                            state=value.get('state',{}).get('properties',{})
                        else:
                            continue  # Only adapter feature replacement events.
                        alerts=set(state.get('alerts',[])); activated=sorted(alerts-known); known=alerts
                        record=dict(received_at=time.time(),event=event,activated=activated,
                            observed_version=state.get('version'),sensor_quality=state.get('sensor_quality'))
                        log.write(json.dumps(record,ensure_ascii=False)+'\n'); log.flush()
                        if activated: print('ALERT:',', '.join(activated),flush=True)
                        else: print('DITTO EVENT', event.get('path'), flush=True)
                except (OSError,ValueError,websocket.WebSocketException) as e:
                    print('RECONNECT',str(e),flush=True); time.sleep(2)
                finally:
                    if ws: ws.close()
    except KeyboardInterrupt: pass
if __name__=='__main__': main()
