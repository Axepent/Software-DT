import argparse
import json
from .api import API
from .model import ROOT, NS, tid

def traverse(api, start):
    seen = set(); todo = [start]; nodes = []; edges = []
    while todo:
        id = todo.pop()
        if id in seen: continue
        seen.add(id); obj = api.thing(id); nodes.append(id)
        for relation, targets in obj.get('attributes',{}).get('relations',{}).items():
            for target in targets:
                edges.append(dict(source=id, relation=relation, target=target))
                if target not in seen: todo.append(target)
    return dict(nodes=nodes, edges=edges)

def main():
    p=argparse.ArgumentParser(); p.add_argument('query', choices=['state','hot','joints','graph'])
    a=p.parse_args(); api=API()
    if a.query=='state': result=api.thing(ROOT)
    elif a.query=='graph': result=traverse(api,ROOT)
    else:
        expr = ('gt(features/state/properties/drive_temperature,70)' if a.query=='hot'
                else 'eq(attributes/kind,"joint")')
        result=api.request('GET','search/things',params={'namespaces':NS,'filter':expr,'option':'size(100)'})
    print(json.dumps(result,ensure_ascii=False,indent=2))
if __name__=='__main__': main()
