from __future__ import annotations

from urllib.parse import urlparse

from akamai_api.edge_auth import AkamaiSession
from requests import HTTPError
from utils.exceptions import setup_logger
from utils.parser import Parser


logger = setup_logger()


class PapiWrapper(AkamaiSession):
    def __init__(self, account_key: str, contract_id: str | None = None, group_id: int | None = None):
        super().__init__()
        self._base_url = f'{self.base_url}/papi/v1'
        self.headers = {'PAPI-Use-Prefixes': 'false',
                        'Accept': 'application/json'}
        self.account_switch_key = account_key
        self.contract_id = contract_id
        self.group_id = group_id

    def get_contracts(self) -> list:
        try:
            response = self.session.get(f'{self._base_url}/contracts', params=self.params, headers=self.headers)
            logger.warning(f'Collecting contract {urlparse(response.url).path:<30} {response.status_code}')
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f'{response.url} {response.status_code}')
        except HTTPError as e:
            logger.error(f'{response.url} {e}')

    def get_contracts_2(self, x, c) -> list:
        self.get_contracts()
        self.group_id = x
