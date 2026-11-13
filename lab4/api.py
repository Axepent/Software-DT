import os
from urllib.parse import quote
import requests

class API:
    def __init__(self):
        self.url = os.getenv('DITTO_URL', 'http://127.0.0.1:8080').rstrip('/')
        self.session = requests.Session()
        self.session.auth = (os.getenv('DITTO_USER', 'ditto'), os.getenv('DITTO_PASSWORD', 'ditto'))
    def request(self, method, path, **kwargs):
        r = self.session.request(method, self.url + '/api/2/' + path, timeout=15, **kwargs)
        r.raise_for_status()
        return r.json() if r.content else None
    def thing(self, thing_id):
        return self.request('GET', 'things/' + quote(thing_id, safe=''))
    def update(self, thing_id, features):
        # PUT preserves JSON null values. Merge-patch would delete null-valued members.
        return self.request('PUT', 'things/' + quote(thing_id, safe='') + '/features', json=features)
