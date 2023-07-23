# Techdocs reference
# https://techdocs.akamai.com/cp-codes/reference/get-cpcode
from __future__ import annotations

from akamai_api.edge_auth import AkamaiSession
from utils import _logging as lg

logger = lg.setup_logger()


class CpCode(AkamaiSession):
    def __init__(self, account_switch_key: str, contract_id: str | None = None, group_id: int | None = None):
        super().__init__(account_switch_key=account_switch_key)
        self._base_url = f'{self.base_url}/cprg/v1/'
        self.headers = {'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'PAPI-Use-Prefixes': 'false',
                        }
        self.account_switch_key = account_switch_key
        self.contract_id = contract_id
        self.group_id = group_id

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

        resp = self.session.get(f'{self._base_url}/cpcodes', params=params, headers=self.headers)
        return resp.status_code, resp.json()

    def get_cpcode(self, cpcode: str) -> tuple:
        resp = self.session.get(f'{self._base_url}cpcodes/{cpcode}', params=self.params, headers=self.headers)
        return resp.status_code, resp.json()


if __name__ == '__main__':
    pass
