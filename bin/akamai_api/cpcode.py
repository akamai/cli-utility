# Techdocs reference
# https://techdocs.akamai.com/cp-codes/reference/get-cpcode
from __future__ import annotations

import logging

from akamai_api.edge_auth import AkamaiSession
from utils import _logging as lg


class CpCode(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 logger: logging.Logger = None):
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
        self.MODULE = f'{self.base_url}/cprg/v1'
        self.headers = {'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'PAPI-Use-Prefixes': 'false',
                        }
        self.account_switch_key = account_switch_key if account_switch_key else None
        self.contract_id = None
        self.group_id = None
        self.logger = logger

    def list_cpcode(self,
                    contract_id: str | None = None,
                    group_id: str | None = None,
                    product_id: str | None = None,
                    cpcode_name: str | None = None) -> tuple:
        params = {}
        if contract_id:
            params['contractId'] = contract_id
        if group_id:
            params['groupId'] = group_id
        if product_id:
            params['productId'] = product_id
        if cpcode_name:
            params['cpcodeName'] = cpcode_name
        if self.account_switch_key:
            params['accountSwitchKey'] = self.account_switch_key

        return self.session.get(f'{self.MODULE}/cpcodes', params=params, headers=self.headers)

    def get_cpcode(self, cpcode: str) -> tuple:
        return self.session.get(f'{self.MODULE}/cpcodes/{cpcode}', params=self.params, headers=self.headers)


if __name__ == '__main__':
    pass
