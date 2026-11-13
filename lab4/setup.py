"""Create missing things; do not reset existing telemetry on repeated execution."""
import json
from pathlib import Path
from urllib.parse import quote
import requests
from .api import API
from .model import models, policy, POLICY

def main():
    api = API()
    api.request('PUT', 'policies/' + quote(POLICY, safe=''), json=policy())
    for id, body in models().items():
        try:
            api.thing(id)
            print('EXISTS', id)
        except requests.HTTPError as e:
            if e.response.status_code != 404: raise
            api.request('PUT', 'things/' + quote(id, safe=''), json=body)
            print('CREATED', id)
    print('11 Things ready. Run: python -m lab4.queries graph')
if __name__ == '__main__': main()
