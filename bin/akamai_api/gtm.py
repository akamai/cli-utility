# Techdocs reference
# https://techdocs.akamai.com/gtm/reference/api
from __future__ import annotations

import logging

from akamai_api.edge_auth import AkamaiSession


class GtmWrapper(AkamaiSession):
    def __init__(self, account_switch_key: str, contract_id: str | None = None, group_id: int | None = None,
                 logger: logging.Logger = None):
        super().__init__()
        self._base_url = f'{self.base_url}/config-gtm/v1'
        self.headers = {'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'PAPI-Use-Prefixes': 'false',
                        }
        self.account_switch_key = account_switch_key
        self.contract_id = contract_id
        self.group_id = group_id
        self.logger = logger

    def list_domains(self) -> tuple:
        resp = self.session.get(f'{self._base_url}/domains', params=self.params, headers=self.headers)
        return resp.status_code, resp.json()

    def get_domain(self, domain: str) -> tuple:
        resp = self.session.get(f'{self._base_url}/domains/{domain}', params=self.params, headers=self.headers)
        if resp.status_code == 406:
            accept_value = resp.json()['minimumMediaTypeRequired']
            resp = self.session.get(f'{self._base_url}/domains/{domain}', params=self.params, headers={'Accept': accept_value})
        return resp.status_code, resp.json()


if __name__ == '__main__':
    pass
