"""Відтворювані MQTT-досліди. За замовчуванням потрібен зовнішній брокер.
--embedded запускає AMQTT тільки для перевірки, а не Mosquitto.
"""
import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def wait_ready(proc, log, timeout=15):
    deadline = time.monotonic()+timeout
    while time.monotonic()<deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'Процес завершився: {log.read_text(encoding="utf-8")}')
        if log.exists() and 'READY |' in log.read_text(encoding='utf-8'):
            return
        time.sleep(.05)
    raise RuntimeError('Timeout очікування READY')


def run_one(name, model_duration, speed, args):
    folder = args.output/name
    if folder.exists():
        raise RuntimeError(f'{folder} вже існує; використайте новий --output')
    folder.mkdir(parents=True)
    broker = ['--host', args.host, '--port', str(args.port)]
    real_duration = model_duration/speed
    processes=[]
    files=[]
    def start(script, extra, logname):
        log=folder/logname
        out=log.open('w',encoding='utf-8')
        files.append(out)
        proc=subprocess.Popen([sys.executable,'-u',str(ROOT/script),*broker,*extra],
                              stdout=out,stderr=subprocess.STDOUT,cwd=ROOT)
        processes.append(proc)
        return proc,log
    try:
        consumer,_=start('state_subscriber.py', ['--duration',str(real_duration+7),
                         '--output',str(folder/'subscriber_states.jsonl')], 'subscriber.log')
        extra=['--duration',str(real_duration+4),'--output',str(folder),
               '--client-id','lab3-'+uuid.uuid4().hex[:16]]
        if not args.influx:
            extra += ['--local-only']
        if name=='outage':
            extra += ['--pause-at','5','--pause-seconds','3']
        service,log=start('twin_service.py',extra,'service.log')
        wait_ready(service,log)
        publisher,_=start('publisher_stream.py',['--duration',str(model_duration),
                         '--speed',str(speed),'--qos','1','--log-dir',str(folder)],'publisher.log')
        if publisher.wait(timeout=real_duration+25):
            raise RuntimeError('Помилка publisher; див. publisher.log')
        if service.wait(timeout=20):
            raise RuntimeError('Помилка service; див. service.log')
        if consumer.wait(timeout=15):
            raise RuntimeError('Помилка subscriber; див. subscriber.log')
        print('DONE | '+name,flush=True)
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
                try: proc.wait(5)
                except subprocess.TimeoutExpired: proc.kill()
        for out in files: out.close()


def run(args):
    plan=[('normal',120 if not args.quick else 12,1),('burst',36,3),('outage',18,1)]
    for name,duration,speed in plan:
        run_one(name,duration,speed,args)
    subprocess.run([sys.executable,str(ROOT/'analyze_pipeline.py'),str(args.output)],check=True)
    (args.output/'environment.json').write_text(json.dumps(dict(
        broker='AMQTT 0.11.3 (test broker)' if args.embedded else 'external (see deployment)',
        influxdb_tested=args.influx, quick=args.quick,
        python=sys.version, platform=sys.platform),indent=2),encoding='utf-8')


async def embedded(args):
    from amqtt.broker import Broker
    broker=Broker({'listeners':{'default':{'type':'tcp','bind':f'127.0.0.1:{args.port}'}},
                   'plugins':{'amqtt.plugins.authentication.AnonymousAuthPlugin':{'allow_anonymous':False}}})
    # AMQTT не накопичує offline-повідомлення анонімних клієнтів.
    # Тестовий плагін приймає довільне непорожнє ім’я, це не production auth.
    os.environ['MQTT_USERNAME'] = 'lab3-test'
    await broker.start()
    try:
        await asyncio.to_thread(run,args)
    finally:
        await broker.shutdown()


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--host',default='127.0.0.1')
    p.add_argument('--port',type=int,default=1883)
    p.add_argument('--output',type=Path,default=ROOT/'results'/'scenarios')
    p.add_argument('--embedded',action='store_true')
    p.add_argument('--influx',action='store_true',help='Писати в справжню InfluxDB; потрібен INFLUX_TOKEN')
    p.add_argument('--quick',action='store_true',help='Скоротити normal зі 120 до 12 с')
    args=p.parse_args()
    args.output=args.output.resolve()
    if args.embedded:
        asyncio.run(embedded(args))
    else:
        run(args)
