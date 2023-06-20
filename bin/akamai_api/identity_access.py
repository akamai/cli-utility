# Techdocs reference
# https://techdocs.akamai.com/iam-api/reference/get-client-account-switch-keys
from __future__ import annotations

import re
import sys

from akamai_api.edge_auth import AkamaiSession
from rich import print_json
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

    def search_account_name(self, value: str | None = None) -> str:
        qry = f'?search={value.upper()}' if value else None

        # this endpoint doesn't use account switch key
        url = f'{self.MODULE}/api-clients/self/account-switch-keys{qry}'
        resp = self.session.get(url, headers=self.headers)
        account_name = []
        if resp.status_code == 200:
            if len(resp.json()) == 0:
                account = value.split(':')[0]
                accounts = self.search_account_name_without_colon(account)
                account_name = []
                for account in accounts:
                    temp_account = re.sub(r'\s', '_', account['accountName'])
                account_name.append(temp_account)
            else:
                for account in resp.json():
                    account_name.append(account['accountName'])
        elif resp.json()['title'] == 'ERROR_NO_SWITCH_CONTEXT':
            sys.exit(logger.error('You do not have permission to lookup other accounts'))
        elif 'WAF deny rule IPBLOCK-BURST' in resp.json()['detail']:
            lg.countdown(540, msg='Oopsie! You just hit rate limit.')
            sys.exit(logger.error(resp.json()['detail']))
        else:
            sys.exit(logger.error(resp.json()['detail']))

        if len(account_name) > 1:
            print_json(data=resp.json())
            sys.exit(logger.error('please use the right account switch key'))
        return account_name

    def search_account_name_without_colon(self, value: str | None = None) -> str:
        qry = f'?search={value.upper()}' if value else None

        # this endpoint doesn't use account switch key
        url = f'{self.MODULE}/api-clients/self/account-switch-keys{qry}'
        resp = self.session.get(url, headers=self.headers)

        if resp.status_code == 200:
            return resp.json()
        elif resp.json()['title'] == 'ERROR_NO_SWITCH_CONTEXT':
            sys.exit(logger.error('You do not have permission to lookup other accounts'))
        elif 'WAF deny rule IPBLOCK-BURST' in resp.json()['detail']:
            lg.countdown(540, msg='Oopsie! You just hit rate limit.')
            sys.exit(logger.error(resp.json()['detail']))
        else:
            sys.exit(logger.error(resp.json()['detail']))
