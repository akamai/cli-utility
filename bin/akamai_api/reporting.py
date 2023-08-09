from __future__ import annotations

import html
import logging
import sys

from akamai_api.edge_auth import AkamaiSession
from bs4 import BeautifulSoup
from rich import print_json
from utils import _logging as lg
from utils import files


class Reporting(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 output: str | None = None,
                 logger: logging.Logger = None):
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
        self.MODULE = f'{self.base_url}/reporting-api/v1'
        self.headers = {'Accept': 'application/json'}
        if output == 'csv':
            self.headers['Content-Type'] = 'text/csv'
        else:
            self.headers['Content-Type'] = 'application/json'
        self.contract_id = self.contract_id
        self.group_id = self.group_id
        self.account_switch_key = account_switch_key
        self.property_id = None
        self.logger = logger

    def list_report(self):
        url = f'{self.MODULE}/reports'
        if self.account_switch_key is not None:
            params = {'accountSwitchKey': self.account_switch_key}
        resp = self.session.get(f'{url}', params=params, headers=self.headers)

        if resp.status_code == 200:
            return resp.json()
        elif 'WAF deny rule IPBLOCK' in resp.json()['detail']:
            self.logger.error(resp.json()['detail'])
            lg.countdown(540, msg='Oopsie! You just hit rate limit.', logger=self.logger)
            sys.exit()
        else:
            self.logger.error(print_json(data=resp.json()))
            return resp.json()

    def hits_by_hostname(self, start: str, end: str, interval: str | None = None):
        url = f'{self.MODULE}/reports/hostname-hits-by-hostname/versions/1/report-data'
        params = {
            'start': start,
            'end': end,
            'interval': interval if interval else 'HOUR',
            'trace': 'true'
        }
        if self.account_switch_key is not None:
            params['accountSwitchKey'] = self.account_switch_key

        payload = {'objectType': 'cpcode',
                   'objectIds': ['all'],
                   'metrics': ['edgeHits'],
                   'limit': 10000}

        resp = self.session.post(f'{url}', json=payload, params=params, headers=self.headers)
        if resp.status_code == 200:
            files.write_json('output/reporting_trace.json', resp.json())
            return resp.json()['data']
        elif 'WAF deny rule IPBLOCK' in resp.json()['detail']:
            self.logger.error(resp.json()['detail'])
            lg.countdown(540, msg='Oopsie! You just hit rate limit.', logger=self.logger)
            sys.exit()

        else:
            self.logger.error(print_json(data=resp.json()))
            return resp.json()

    def hits_by_url(self, start: str, end: str, cpcodes):
        url = f'{self.MODULE}/reports/urlhits-by-url/versions/1/report-data'
        params = {
            'start': start,
            'end': end,
            'limit': 10000
        }
        if self.account_switch_key is not None:
            params['accountSwitchKey'] = self.account_switch_key

        payload = {
            'objectIds': cpcodes,
            'metrics': [
                'allEdgeHits',
                'allHitsOffload',
                'allOriginHits'
            ]
        }

        resp = self.session.post(url, json=payload, params=params, headers=self.headers)
        if resp.status_code == 200:
            files.write_json('output/reporting_trace.json', resp.json())
            return resp.status_code, resp.json()['data']
        elif 400 <= resp.status_code <= 499:
            return resp.status_code, resp.json()
        elif resp.status_code == 504:
            soup = BeautifulSoup(resp.text, 'html.parser')
            encoded_reference = soup.body.get_text()
            decoded_reference = html.unescape(encoded_reference)
            self.logger.error(decoded_reference)
            return resp.status_code, resp.text
        else:
            self.logger.error(f'{resp.status_code=} {resp.text}')
            return resp.status_code, resp.json()

    def traffic_by_response_class(self, start: str, end: str,
                                  interval: str | None = None,
                                  cpcode: str | None = None):

        url = f'{self.MODULE}/reports/traffic-by-responseclass/versions/1/report-data'
        params = {
            'start': start,
            'end': end,
            'interval': interval if interval else 'HOUR'
        }
        if self.account_switch_key is not None:
            params['accountSwitchKey'] = self.account_switch_key

        payload = {'metrics': ['edgeHits', 'edgeHitsPercent', 'originHits', 'originHitsPercent']}

        if cpcode:
            payload['objectIds'] = [cpcode]
        else:
            payload['allObjectIds'] = True

        resp = self.session.post(url, json=payload, params=params, headers=self.headers)
        self.logger.debug(payload)
        self.logger.debug(f'{cpcode} {resp.status_code} {resp.url}')

        if resp.status_code == 200:
            if cpcode:
                return resp.json()['data']
            else:
                files.write_json('output/reporting_trace.json', resp.json())
                return resp.json()
        elif resp.status_code == 403:
            self.logger.error(resp.json()['errors'])
        elif resp.status_code == 404:
            self.logger.error(resp.json()['errors'])
        elif resp.status_code == 429:
            self.logger.error(f"{cpcode:>10} {resp.json()['detail']}")
        else:
            self.logger.error(cpcode)
            print_json(data=resp.json())
        return None


if __name__ == '__main__':
    pass
