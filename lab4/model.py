"""Thing graph and mapping of the processed Lab 3 state (not raw telemetry)."""
import copy
NS = 'ua.lab4'
ROOT = f'{NS}:mannequin-01'
POLICY = f'{NS}:lab4'
JOINTS = ['shoulder', 'elbow', 'hip', 'knee', 'head', 'torso']

def tid(name):
    return f'{NS}:{name}'

def models():
    result = {}
    parts = JOINTS + ['drive-01', 'power', 'safety', 'controller']
    for name in ['mannequin-01'] + parts:
        relations = ({'contains': [tid(n) for n in parts]} if name == 'mannequin-01'
                     else {'partOf': [ROOT]})
        if name == 'controller': relations['controls'] = [tid('drive-01')]
        if name == 'power': relations['powers'] = [tid('drive-01'), tid('controller')]
        if name == 'drive-01': relations['actuates'] = [tid('shoulder')]
        kind = 'joint' if name in JOINTS else name.split('-')[0]
        attrs = dict(name=name, kind=kind, relations=relations)
        if name in JOINTS:
            attrs.update(sensorId=f'S-{JOINTS.index(name)+1:02}', unit='deg')
        result[tid(name)] = dict(thingId=tid(name), policyId=POLICY,
            attributes=attrs, features={'state': {'properties': {'initialized': False}}})
    return result

def policy():
    return {'entries': {'OWNER': {'subjects': {'nginx:ditto': {'type': 'local lab operator'}},
            'resources': {k: {'grant': ['READ', 'WRITE'], 'revoke': []}
                          for k in ['thing:/', 'policy:/', 'message:/']}}}}

def project(state):
    if state.get('device_id') != 'mannequin-01':
        raise ValueError('Unsupported device_id')
    if type(state.get('version')) is not int or state['version'] < 1:
        raise ValueError('Invalid state version')
    meta = {k: state[k] for k in ['run_id', 'version', 'updated_at', 'event_time',
                                 'connection_status', 'broker_connected']}
    quality = state['sensor_quality']
    result = {ROOT: copy.deepcopy(state)}
    for i, name in enumerate(JOINTS, 1):
        sid = f'S-{i:02}'
        result[tid(name)] = dict(meta, angle=state['joint_angles'][sid],
                                 sensor_quality={sid: quality[sid]})
    result[tid('drive-01')] = dict(meta, **{k: state[k] for k in [
        'drive_current', 'drive_temperature', 'current_mean_100ms', 'current_max_100ms',
        'current_mean_1s', 'current_max_1s']}, sensor_quality={s: quality[s] for s in ['S-07','S-08']})
    result[tid('power')] = dict(meta, supply_voltage=state['supply_voltage'],
                              sensor_quality={'S-09': quality['S-09']})
    result[tid('safety')] = dict(meta, limit_switch=state['limit_switch'],
        emergency_input=state['emergency_input'], alerts=state['alerts'],
        sensor_quality={s: quality[s] for s in ['S-10','S-11']})
    # Controller has descriptive relationships only: no invented controller measurements.
    return {k: {'state': {'properties': dict(v, initialized=True)}} for k,v in result.items()}
