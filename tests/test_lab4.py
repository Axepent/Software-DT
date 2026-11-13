import copy
import unittest
from unittest.mock import Mock
from twin_state import TwinPipeline
from lab4.model import models, project, ROOT, tid
from lab4.queries import traverse
from lab4.api import API

class Lab4Test(unittest.TestCase):
    def state(self):
        states=[]
        p=TwinPipeline(on_state=states.append)
        p.emit(1800000000,1800000000,'test')
        return states[-1]
    def test_missing_measurements_stay_null(self):
        result=project(self.state())
        self.assertEqual(len(result),10)
        self.assertIsNone(result[tid('shoulder')]['state']['properties']['angle'])
        self.assertEqual(result[tid('shoulder')]['state']['properties']['sensor_quality']['S-01']['status'],'missing')
        self.assertNotIn(tid('controller'),result)
    def test_graph_cycles_and_all_targets(self):
        m=models(); api=Mock(); api.thing.side_effect=lambda id:m[id]
        graph=traverse(api,ROOT)
        self.assertEqual(len(graph['nodes']),11)
        self.assertEqual(api.thing.call_count,11)
        self.assertTrue(all(e['target'] in m for e in graph['edges']))
    def test_mapping_preserves_stale(self):
        s=self.state(); s['drive_temperature']=80
        s['sensor_quality']['S-08']={'status':'stale','measured_at':None,'age_s':8}
        before=copy.deepcopy(s); result=project(s)
        self.assertEqual(s,before)
        self.assertEqual(result[tid('drive-01')]['state']['properties']['sensor_quality']['S-08']['status'],'stale')
        self.assertEqual(result[ROOT]['state']['properties']['version'],s['version'])
    def test_http_uses_put_for_null(self):
        api=API(); api.session=Mock(); api.session.request.return_value.content=b''
        body={'state':{'properties':{'angle':None}}}
        api.update(tid('shoulder'),body)
        args,kwargs=api.session.request.call_args
        self.assertEqual(args[0],'PUT'); self.assertEqual(kwargs['json'],body)
    def test_wrong_device_rejected(self):
        s=self.state(); s['device_id']='another'
        with self.assertRaises(ValueError): project(s)
