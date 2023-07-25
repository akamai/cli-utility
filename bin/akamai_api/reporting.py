from __future__ import annotations

import sys

from akamai_api.edge_auth import AkamaiSession
from rich import print_json
from utils import _logging as lg
from utils import files

logger = lg.setup_logger()


class Reporting(AkamaiSession):
    def __init__(self, output: str | None = None, account_switch_key: str | None = None):
        super().__init__()
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

    def list_report(self):
        url = f'{self.MODULE}/reports'
        if self.account_switch_key is not None:
            params = {'accountSwitchKey': self.account_switch_key}
        resp = self.session.get(f'{url}', params=params, headers=self.headers)

        if resp.status_code == 200:
            return resp.json()
        elif 'WAF deny rule IPBLOCK' in resp.json()['detail']:
            lg.countdown(540, msg='Oopsie! You just hit rate limit.')
            sys.exit(logger.error(resp.json()['detail']))
        else:
            logger.error(print_json(data=resp.json()))
            return resp.json()

    def hits_by_hostname(self, start: str, end: str):
        url = f'{self.MODULE}/reports/hostname-hits-by-hostname/versions/1/report-data'
        params = {
            'start': start,
            'end': end,
            'internal': 'DAY',
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
            lg.countdown(540, msg='Oopsie! You just hit rate limit.')
            sys.exit(logger.error(resp.json()['detail']))
        else:
            logger.error(print_json(data=resp.json()))
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
        elif resp.status_code == 403:
            print_json(data=resp.json())
            return resp.status_code, resp.json()
        elif 'WAF deny rule IPBLOCK' in resp.json()['data']:
            lg.countdown(540, msg='Oopsie! You just hit rate limit.')
            sys.exit(logger.error(print_json(data=resp.json())))
        else:
            logger.error(print_json(data=resp.json()))
            return resp.status_code, resp.json()

    def traffic_by_response_class(self, start: str, end: str, interval: str, cpcode: str | None = None):

        url = f'{self.MODULE}/reports/traffic-by-responseclass/versions/1/report-data'
        params = {
            'start': start,
            'end': end,
            'interval': interval
        }
        if self.account_switch_key is not None:
            params['accountSwitchKey'] = self.account_switch_key

        payload = {'metrics': ['edgeHits', 'edgeHitsPercent', 'originHits', 'originHitsPercent']}

        if cpcode:
            payload['objectIds'] = [cpcode]
        else:
            payload['allObjectIds'] = True

        resp = self.session.post(url, json=payload, params=params, headers=self.headers)
        logger.debug(f'{cpcode} {resp.status_code} {resp.url}')

        if resp.status_code == 200:
            # files.write_json('output/reporting_trace.json', resp.json())
            return resp.json()['data']
        elif resp.status_code == 403:
            logger.error(resp.json()['errors'])
        elif resp.status_code == 404:
            logger.error(resp.json()['errors'])
        elif resp.status_code == 429:
            logger.error(f"{cpcode:>10} {resp.json()['detail']}")
        else:
            logger.error(cpcode)
            print_json(data=resp.json())
        return None


if __name__ == '__main__':
    pass
