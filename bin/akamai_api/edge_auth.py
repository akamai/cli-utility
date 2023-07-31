from __future__ import annotations

import logging
import os
import re
import sys
from configparser import NoOptionError
from configparser import NoSectionError
from pathlib import Path

import requests
from akamai.edgegrid import EdgeGridAuth
from akamai.edgegrid import EdgeRc


logger = logging.getLogger(__name__)


class AkamaiSession:
    def __init__(self, edgerc_file: str | None = None,
                 section: str | None = None,
                 account_switch_key: str | None = None,
                 cookies: str | None = None,
                 contract_id: int | None = None,
                 group_id: int | None = None):

        self.edgerc_file = edgerc_file if edgerc_file else EdgeRc(f'{str(Path.home())}/.edgerc')
        self.account_switch_key = account_switch_key if account_switch_key else None
        self.contract_id = contract_id if contract_id else None
        self.group_id = group_id if group_id else None
        self.section = section if section else 'default'
        self.cookies = self.update_acc_cookie(cookies)

        try:
            self.host = self.edgerc_file.get(self.section, 'host')
            self.base_url = f'https://{self.host}'
            self.session = requests.Session()
            self.session.auth = EdgeGridAuth.from_edgerc(self.edgerc_file, self.section)
        except NoSectionError:
            sys.exit(logger.error(f'edgerc section "{self.section}" not found'))

    @property
    def params(self) -> dict:
        return {'accountSwitchKey': self.account_switch_key} if self.account_switch_key else {}

    def form_url(self, url: str) -> str:
        account_switch_key = f'&accountSwitchKey={self.account_switch_key}' if self.account_switch_key is not None else ''
        if '?' in url:
            return f'{url}{account_switch_key}'
        else:
            account_switch_key = account_switch_key.translate(account_switch_key.maketrans('&', '?'))
            return f'{url}{account_switch_key}'

    def update_account_key(self, account_key: str) -> None:
        self.account_switch_key = account_key

    def update_acc_cookie(self, cookies: str) -> dict:
        '''
        Required for pulsar API
        https://ac-aloha.akamai.com/home/ls/content/5296164953915392/polling-the-pulsar-api-for-pleasure-profit

        This is not required on .edgerc
        '''

        self.cookies = {}
        if cookies:
            match_xsrf = re.search(r'XSRF-TOKEN=([^;\s]+)', cookies)
            if match_xsrf:
                self.cookies['XSRF-TOKEN'] = match_xsrf.group(1)

            match_sso = re.search(r'AKASSO=([^;\s]+)', cookies)
            if match_xsrf:
                self.cookies['AKASSO'] = match_sso.group(1)

            match_token = re.search(r'AKATOKEN=([^;\s]+)', cookies)
            if match_token:
                self.cookies['AKATOKEN'] = match_token.group(1)
        else:
            try:
                self.cookies['XSRF-TOKEN'] = self.edgerc_file.get(self.section, 'XSRF-TOKEN')
            except NoOptionError:
                pass

            try:
                self.cookies['AKASSO'] = self.edgerc_file.get(self.section, 'AKASSO')
            except NoOptionError:
                pass

            try:
                self.cookies['AKATOKEN'] = self.edgerc_file.get(self.section, 'AKATOKEN')
            except NoOptionError:
                pass

        return self.cookies


if __name__ == '__main__':
    pass
