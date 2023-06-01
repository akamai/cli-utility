# Techdocs reference
# https://techdocs.akamai.com/iam-api/reference/get-client-account-switch-keys
from __future__ import annotations

import sys

from akamai_api.edge_auth import AkamaiSession
from utils import _logging as lg


logger = lg.setup_logger()


class IdentityAccessManagement(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None, section: str | None = None):
        super().__init__(account_switch_key=account_switch_key, section=section)
        self.MODULE = f'{self.base_url}/identity-management/v3'
        self.headers = {'Accept': 'application/json'}
        self.contract_id = self.contract_id
        self.group_id = self.group_id
        self.account_switch_key = account_switch_key
        self.property_id = None

    def search_account_name(self, value: str) -> str:
        qry = f'?search={value.upper()}'
        url = self.form_url(f'{self.MODULE}/api-clients/self/account-switch-keys{qry}')
        resp = self.session.get(url, headers=self.headers)
        if resp.status_code == 200:
            return resp.json()
        elif resp.json()['title'] == 'ERROR_NO_SWITCH_CONTEXT':
            sys.exit(logger.error('You do not have permission to lookup other accounts'))
        elif 'WAF deny rule IPBLOCK-BURST' in resp.json()['detail']:
            lg.countdown(540)
            sys.exit(logger.error(resp.json()['detail']))
        else:
            sys.exit(logger.error(resp.json()['detail']))
