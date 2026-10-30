import json
import tempfile
import unittest
from twin_state import TwinPipeline, iso
from mqtt_common import validate, topic_for
from timeseries_store import HistoryStore
from sensor_emulator import SENSORS
BASE = 1800000000.0


def sample(sid='S-01', value=0., t=BASE, seq=1, run='run1'):
    return dict(schema_version=1, run_id=run, device_id='mannequin-01', sample_id=seq,
                sensor_id=sid, timestamp=iso(t), sim_time_s=0., value=value,
                unit=SENSORS[sid].unit, condition='normal', sent_at=iso(t))


class PipelineTest(unittest.TestCase):
    def setup_pipeline(self, **kwargs):
        self.states, self.raw, self.metrics = [], [], []
        self.p = TwinPipeline(on_state=self.states.append, on_raw=self.raw.append,
                              on_metric=self.metrics.append, **kwargs)
        return self.p

    def test_reorder_and_dedup(self):
        p = self.setup_pipeline()
        a, b = sample(t=BASE+.02, value=0., seq=1), sample(t=BASE+.08, value=1., seq=2)
        p.ingest(b, BASE+.09)
        p.ingest(a, BASE+.10)
        p.ingest(a, BASE+.10)
        p.advance(BASE+.21)
        self.assertEqual([r['data']['sample_id'] for r in self.raw], [1,2])
        self.assertEqual(p.counts['duplicate'], 1)
        self.assertEqual(p.last['S-01']['value'], 1.)
        self.assertEqual(len(self.metrics), 2)

    def test_late_does_not_rollback(self):
        p = self.setup_pipeline()
        p.ingest(sample(t=BASE+.05, value=5), BASE+.05)
        p.advance(BASE+.25)
        p.ingest(sample(t=BASE+.03, value=2, seq=2), BASE+.26)
        p.advance(BASE+.4)
        self.assertEqual(p.last['S-01']['value'], 5)
        self.assertEqual(p.counts['late'],1)

    def test_missing_stale_and_events_latched(self):
        p = self.setup_pipeline()
        p.ingest(sample('S-08', 25., BASE+.01), BASE+.01)
        p.ingest(sample('S-10', False, BASE+.01, 2), BASE+.01)
        p.advance(BASE+.3)
        self.assertIsNone(self.states[-1]['joint_angles']['S-01'])
        p.advance(BASE+4)
        self.assertEqual(self.states[-1]['sensor_quality']['S-08']['status'], 'stale')
        self.assertEqual(self.states[-1]['sensor_quality']['S-10']['status'], 'latched')
        p.broker_status(False, BASE+4)
        self.assertEqual(self.states[-1]['sensor_quality']['S-10']['status'], 'stale')

    def test_angular_outlier_and_real_overload(self):
        p = self.setup_pipeline()
        p.ingest(sample(t=BASE+.01), BASE+.01)
        p.ingest(sample(t=BASE+.02,value=80,seq=2), BASE+.02)
        p.ingest(sample('S-07',4.2,BASE+.03,3), BASE+.03)
        p.advance(BASE+.3)
        self.assertEqual(p.counts['outlier'],1)
        self.assertEqual(p.last['S-01']['value'],0)
        self.assertIn('overcurrent', self.states[-1]['alerts'])

    def test_rules_and_aggregates(self):
        p = self.setup_pipeline()
        for i, (sid,v,t) in enumerate([('S-01',0.,.01), ('S-11',False,.02),
               ('S-07',1.,.03), ('S-07',3.,.04), ('S-01',2.,.08), ('S-11',True,.09)],1):
            p.ingest(sample(sid,v,BASE+t,i),BASE+t)
        p.advance(BASE+.25)
        reasons={s['reason'] for s in self.states}
        self.assertTrue({'periodic','event','threshold'} <= reasons)
        self.assertEqual(self.states[-1]['current_mean_1s'],2.)
        self.assertEqual(self.states[-1]['current_max_1s'],3.)
        self.assertEqual(len(self.metrics),6)
        self.assertTrue(all(m['latency_ms']>=0 for m in self.metrics))

    def test_old_run_and_session_reset(self):
        p = self.setup_pipeline()
        p.ingest(sample(t=BASE+.01), BASE+.01)
        p.advance(BASE+.3)
        p.ingest(sample('S-08',28.,BASE+.31,1,'run2'),BASE+.31)
        p.advance(BASE+.6)
        self.assertIsNone(self.states[-1]['joint_angles']['S-01'])
        p.ingest(sample(t=BASE+.61,seq=9,run='run1'),BASE+.61)
        p.advance(BASE+.9)
        self.assertEqual(p.run_id,'run2')
        self.assertEqual(p.counts['old_run'],1)

    def test_validation_and_future_and_capacity(self):
        with self.assertRaises(ValueError):
            validate(topic_for('mannequin-01','S-01'),json.dumps(sample(value=True)).encode())
        p=self.setup_pipeline(capacity=1)
        p.ingest(sample(t=BASE+10),BASE)
        self.assertEqual(p.counts['future'],1)
        p.ingest(sample(t=BASE+.01),BASE+.01)
        p.ingest(sample(t=BASE+.02,seq=2),BASE+.02)
        self.assertEqual(p.counts['overflow'],1)

    def test_store_outbox_persistence(self):
        with tempfile.TemporaryDirectory() as folder:
            s=HistoryStore(folder)
            s.raw(dict(data=sample('S-10',False),received_at=iso(BASE),outcome='applied'))
            s.flush()
            self.assertEqual(s.pending(),1)
            self.assertEqual(s.close()['outbox_pending'],1)
            s=HistoryStore(folder)
            self.assertEqual(s.pending(),1)
            payload=s.db.execute('SELECT line FROM outbox').fetchone()[0]
            self.assertIn('discrete_value=false',payload)
            s.close()

class StorageHTTPTest(unittest.TestCase):
    def test_http_retry_and_queue_recovery(self):
        import http.server
        import threading
        import time
        bodies=[]
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                bodies.append((self.path,self.headers.get('Authorization'),
                               self.rfile.read(int(self.headers['Content-Length']))))
                self.send_response(503 if len(bodies)==1 else 204)
                self.end_headers()
            def log_message(self,*args): pass
        server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as folder:
                store=HistoryStore(folder)
                store.raw(dict(data=sample(),received_at=iso(BASE),outcome='applied'))
                store.close()
                store=HistoryStore(folder,url=f'http://127.0.0.1:{server.server_port}',token='test-token')
                deadline=time.monotonic()+5
                while store.pending() and time.monotonic()<deadline:
                    time.sleep(.05)
                self.assertEqual(store.pending(),0)
                self.assertEqual(store.close()['influx_written'],1)
                self.assertGreaterEqual(len(bodies),2)
                self.assertEqual(bodies[0],bodies[1])
                self.assertIn('precision=ns',bodies[0][0])
                self.assertEqual(bodies[0][1],'Token test-token')
        finally:
            server.shutdown()
            server.server_close()


if __name__=='__main__':
    unittest.main()
