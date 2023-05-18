from __future__ import annotations

from akamai_api.edge_auth import AkamaiSession
from rich import print_json
from utils import files
from utils._logging import setup_logger

logger = setup_logger()


class Reporting(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None):
        super().__init__()
        self.MODULE = f'{self.base_url}/reporting-api/v1'
        self.headers = {'Accept': 'application/json',
                        'Content-Type': 'application/json'}
        self.contract_id = self.contract_id
        self.group_id = self.group_id
        self.account_switch_key = account_switch_key
        self.property_id = None

    def list_report(self):
        response = self.session.get(f'{self.MODULE}/reports', headers=self.headers)
        if response.status_code == 200:
            return response.json()

    def hits_by_hostname(self, start: str, end: str):
        url = f'{self.MODULE}/reports/hostname-hits-by-hostname/versions/1/report-data'
        # start = '2023-02-17T00:00:00Z'
        # end = '2023-05-17T00:00:00Z'
        query_params = f'?start={start}&end={end}&internal=DAY&trace=true'
        url = self.form_url(f'{url}{query_params}')
        payload = {'objectType': 'cpcode',
                   'objectIds': ['all'],
                   'metrics': ['edgeHits'],
                   'limit': 10000}

        response = self.session.post(f'{url}', json=payload, headers=self.headers)
        if response.status_code == 200:
            files.write_json('output/reporting_trace.json', response.json())
            return response.json()['data']
        else:
            logger.info(print_json(data=response.json()))
            return response.json()


if __name__ == '__main__':
    pass
