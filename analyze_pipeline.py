"""Таблиці та графіки з фактичних журналів MQTT-дослідів."""
import argparse
import csv
import json
import statistics
from pathlib import Path
from twin_state import seconds


def records(path):
    with path.open(encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]


def analyze(folder):
    summaries=[]
    graph_data=[]
    for directory in sorted(folder.iterdir()):
        if not directory.is_dir() or not (directory/'service_summary.json').exists():
            continue
        sent=[]
        for file in directory.glob('sent_*.csv'):
            with file.open(encoding='utf-8-sig',newline='') as f:
                sent.extend(csv.DictReader(f))
        raw=records(directory/'telemetry.jsonl')
        states=records(directory/'states.jsonl')
        delivered=records(directory/'subscriber_states.jsonl') if (directory/'subscriber_states.jsonl').exists() else []
        with (directory/'latency.csv').open(encoding='utf-8',newline='') as f:
            metrics=list(csv.DictReader(f))
        ids=lambda rows:{(r['run_id'],int(r['sample_id'])) for r in rows}
        sent_ids=ids(sent)
        received_ids=ids([r['data'] for r in raw])
        visible=[r for r in metrics if r.get('representation') != 'superseded']
        represented_ids=ids(visible)
        lat=[float(r['latency_ms']) for r in visible if r['clock_valid']=='True']
        # Тривалість активного входу, а не всього часу життя сервісу.
        times=[seconds(r['received_at']) for r in raw]
        interval=max(times)-min(times) if len(times)>1 else 0
        bins={}
        for t in times:
            index=int(t-min(times))
            bins[index]=bins.get(index,0)+1
        svc=json.loads((directory/'service_summary.json').read_text())
        reasons={}
        for s in states:
            reasons[s['reason']]=reasons.get(s['reason'],0)+1
        state_ids={(s['run_id'],s['version']) for s in states}
        delivered_ids={(r['state']['run_id'],r['state']['version']) for r in delivered}
        matched=state_ids & delivered_ids
        row=dict(scenario=directory.name,sent=len(sent_ids),received_unique=len(received_ids),
                 missing=len(sent_ids-received_ids),represented=len(represented_ids),
                 superseded=sum(r.get('representation')=='superseded' for r in metrics),
                 late=svc.get('late',0),outlier=svc.get('outlier',0),
                 invalid=svc.get('invalid',0),overflow=svc.get('overflow',0)+svc.get('callback_overflow',0),
                 states=len(states),notifications_unique=len(matched),
                 notifications_missing=len(state_ids-delivered_ids),
                 notifications_total_including_retained=len(delivered),
                 input_interval_s=round(interval,3),
                 input_rate_msg_s=round(len(raw)/interval,2) if interval else 0,
                 peak_1s_msg=max(bins.values(),default=0),
                 latency_mean_ms=round(statistics.mean(lat),3) if lat else None,
                 latency_max_ms=round(max(lat),3) if lat else None,
                 clock_errors=sum(r['clock_valid']!='True' for r in metrics),
                 outbox_pending=svc['outbox_pending'],reasons=reasons)
        summaries.append(row)
        graph_data.append((directory.name,visible,bins))
    (folder/'summary.json').write_text(json.dumps(summaries,indent=2,ensure_ascii=False),encoding='utf-8')
    if summaries:
        keys=[k for k in summaries[0] if k!='reasons']
        with (folder/'summary.csv').open('w',encoding='utf-8-sig',newline='') as f:
            w=csv.DictWriter(f,fieldnames=keys,extrasaction='ignore')
            w.writeheader();w.writerows(summaries)
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        fig,axes=plt.subplots(2,1,figsize=(10,7),layout='constrained')
        for name,metrics,bins in graph_data:
            label={'normal':'Штатний режим','burst':'Підвищене навантаження','outage':'Розрив зв’язку'}[name]
            if metrics:
                base=seconds(metrics[0]['updated_at'])
                axes[0].plot([seconds(m['updated_at'])-base for m in metrics],
                             [float(m['latency_ms']) for m in metrics],'.',markersize=1,label=label,alpha=.5)
            ticks=range(max(bins,default=0)+1)
            axes[1].plot(ticks,[bins.get(k,0) for k in ticks],label=label)
        axes[0].axhline(500,color='red',linestyle='--',label='Межа 500 мс (штатний режим)')
        axes[0].set(ylabel='Затримка оновлення стану, мс',xlabel='Час від першого оновлення, с')
        axes[1].set(ylabel='Прийнято повідомлень за 1 с',xlabel='Час від першого приймання, с')
        for ax in axes:
            ax.grid(alpha=.2);ax.legend()
        fig.savefig(folder/'pipeline_metrics.png',dpi=160)
        plt.close(fig)
    except ImportError:
        print('Для графіків установіть matplotlib')
    print(json.dumps(summaries,indent=2,ensure_ascii=False))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('folder',type=Path)
    analyze(p.parse_args().folder)
