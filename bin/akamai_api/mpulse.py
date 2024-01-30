# Techdocs reference
# https://techdocs.akamai.com/mpulse/reference/api
from __future__ import annotations

import logging

import requests


class mPulse():
    def __init__(self,
                 apikey: str,
                 token: str | None = None,
                 logger: logging.Logger = None):
        self._base_url = 'https://mpulse.soasta.com/concerto'
        self._version = f'/mpulse/api/v2/{apikey}'
        self.logger = logger
        self.apikey = apikey
        self.token = token
        self.headers = {'Authentication': self.token,
                        'accept': 'application/json'}
        self.session = requests.Session()

    def generate_token(self, tenant: str):
        """
        Authenticates and generates a token.
        The security token expires after five hours of inactivity.
        """
        url = 'services/rest/RepositoryService/v1/Tokens'
        payload = {'tenant': tenant,
                   'apiToken': self.apikey}
        headers = {'accept': 'application/json',
                   'content-type': 'application/json'}
        resp = self.session.put(f'{self._base_url}/{url}', json=payload, headers=headers)
        return resp

    def list(self, type: str):
        """
        Gets a list of repository objects, filtered by attributes.
        """
        url = f'services/rest/RepositoryService/v1/Objects/{type}'
        self.headers['X-Auth-Token'] = self.token
        resp = self.session.get(f'{self._base_url}/{url}', headers=self.headers)
        return resp

    def get_url(self,
                date_start: str | None = '2023-09-01',
                date_to: str | None = '2023-09-30'):
        params = {'format': 'json'}
        params['dimension'] = 'url'
        params['date-start'] = date_start
        params['date-end'] = date_to
        params['date-comparator'] = 'Between'
        resp = self.session.get(f'{self._base_url}{self._version}/dimension-values', params=params, headers=self.headers)
        return resp

    def pageload_overtime(self,
                date_start: str | None = '2023-09-01',
                date_to: str | None = '2023-10-01'):
        params = {'format': 'json'}
        params['series-format'] = 'json'
        params['percentile'] = 10
        # params['beacon-type'] = 'page view'

        params['date-start'] = date_start
        params['date-end'] = date_to
        params['date-comparator'] = 'Between'
        # params['interval'] = '1_day'
        resp = self.session.get(f'{self._base_url}{self._version}/timers-metrics', params=params, headers=self.headers)
        # resp = self.session.get(f'{self._base_url}{self._version}/summary', params=params, headers=self.headers)
        return resp


if __name__ == '__main__':
    pass
