# Techdocs reference
# https://techdocs.akamai.com/network-lists/reference/api-summary
from __future__ import annotations

import logging
import sys

from akamai_api.edge_auth import AkamaiSession
from boltons.iterutils import remap
from rich import print_json
from utils import _logging as lg
from utils import files


class NetworkList(AkamaiSession):
    def __init__(self,
                 account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 logger: logging.Logger = None):
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
        self.MODULE = f'{self.base_url}/network-list/v2'
        self.headers = {'Accept': 'application/json',
                        'Content-Type': 'application/json'}
        self.contract_id = self.contract_id
        self.group_id = self.group_id
        self.config_id = None
        self.account_switch_key = account_switch_key
        self.logger = logger

    def get_all_network_list(self):
        url = self.form_url(f'{self.MODULE}/network-lists')
        response = self.session.get(url, headers=self.headers)
        return response.status_code, response.json()

    def get_network_list(self, id: str):
        url = self.form_url(f'{self.MODULE}/network-lists/{id}')
        response = self.session.get(url, headers=self.headers)
        self.logger.debug(response.status_code)
        # print_json(data=response.json())
        return response.status_code, response.json()

    def update_network_list(self, id: str, payload: dict):
        url = self.form_url(f'{self.MODULE}/network-lists/{id}')
        response = self.session.put(url, json=payload, headers=self.headers)
        return response.status_code, response.json()


if __name__ == '__main__':
    pass
