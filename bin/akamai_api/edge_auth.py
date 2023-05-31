from __future__ import annotations

from pathlib import Path

import requests
from akamai.edgegrid import EdgeGridAuth
from akamai.edgegrid import EdgeRc
from utils import _logging as lg


logger = lg.setup_logger()


class AkamaiSession:
    def __init__(self, edgerc_file: str | None = None,
                 section: str | None = None,
                 account_switch_key: str | None = None,
                 contract_id: int | None = None,
                 group_id: int | None = None):

        self.account_switch_key = account_switch_key if account_switch_key else None
        self.contract_id = contract_id if contract_id else None
        self.group_id = group_id if group_id else None

        home = str(Path.home())
        edgerc_file = EdgeRc(f'{home}/.edgerc')

        if not section:
            section = 'default'
        self.host = edgerc_file.get(section, 'host')
        self.base_url = f'https://{self.host}'
        self.session = requests.Session()
        self.session.auth = EdgeGridAuth.from_edgerc(edgerc_file, section)

        # required for pulsar API
        # https://ac-aloha.akamai.com/home/ls/content/5296164953915392/polling-the-pulsar-api-for-pleasure-profit

        self.cookies = {}
        self.cookies['AKASSO'] = edgerc_file.get(section, 'AKASSO')
        self.cookies['XSRF-TOKEN'] = edgerc_file.get(section, 'XSRF-TOKEN')
        self.cookies['AKATOKEN'] = edgerc_file.get(section, 'AKATOKEN')

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


if __name__ == '__main__':
    pass
