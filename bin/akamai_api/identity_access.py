# Techdocs reference
# https://techdocs.akamai.com/iam-api/reference/get-client-account-switch-keys
from __future__ import annotations

import sys

from akamai_api.edge_auth import AkamaiSession
from utils import _logging as lg


logger = lg.setup_logger()


class IdentityAccessManagement(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None):
        super().__init__()
        self.MODULE = f'{self.base_url}/identity-management/v3'
        self.headers = {'Accept': 'application/json'}
        self.contract_id = self.contract_id
        self.group_id = self.group_id
        self.account_switch_key = account_switch_key
        self.property_id = None

    def search_account_name(self, value: str) -> str:
        qry = f'?search={value.upper()}'
        url = f'{self.MODULE}/api-clients/self/account-switch-keys{qry}'
        resp = self.session.get(url, headers=self.headers)
        if resp.status_code == 200:
            return resp.json()
        else:
            if 'WAF deny rule IPBLOCK-BURST' in resp.json()['detail']:
                lg.countdown(540)
                sys.exit(logger.error(resp.json()['detail']))
            else:
                sys.exit(logger.error(resp.json()['detail']))
