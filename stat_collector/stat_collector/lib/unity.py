# -*- coding: UTF-8 -*-
"""Abstracts the EMC Unity API"""
import requests

# Disable those annoying warnings...
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Unity:
    """An extreamly thin wrapper around ``requests`` to accommodate the EMC Unity's
    use of a anti-CSRF token.
    """
    def __init__(self, ip_addr, username, password, verify=False):
        self.ip_addr = ip_addr
        self.creds = (username, password)
        self.verify = verify
        self.headers = {'X-EMC-REST-CLIENT' : "true"}
        self._session = requests.Session()
        self._login()

    def _extract_csrf_token(self, response):
        """Pull the anti CSRF token from the Unity response"""
        csrf_token = response.headers.get('emc-csrf-token', None)
        if csrf_token:
            self.headers['EMC-CSRF-TOKEN'] = csrf_token

    def _call(self, method, endpoint, json=None, params=None):
        if not endpoint.startswith('/'):
            endpoint = '/{}'.format(endpoint)
        url = 'https://{}{}'.format(self.ip_addr, endpoint)
        caller = getattr(self._session, method.lower())
        resp = caller(url, json=json, params=params, headers=self.headers, auth=self.creds, verify=self.verify)
        self._extract_csrf_token(resp)
        resp.raise_for_status()
        return resp

    def get(self, endpoint, params=None, json=None):
        return self._call(method='get', endpoint=endpoint, params=params)

    def post(self, endpoint, params=None, json=None):
        return self._call(method='post', endpoint=endpoint, params=params, json=json)

    def put(self, endpoint, params=None, json=None):
        return self._call(method='put', endpoint=endpoint, params=params, json=json)

    def delete(self, endpoint, params=None, json=None):
        return self._call(method='delete', endpoint=endpoint, params=params, json=json)

    def _login(self):
        """Must call before performing any POST/DELETE/PUT requests"""
        self.get('/api/types/loginSessionInfo')

    def close(self):
        """Terminate the HTTP session with the Unity server"""
        body = {"localCleanupOnly" : True}
        self.post('/api/types/loginSessionInfo/action/logout', json=body)
        self._session.close()
