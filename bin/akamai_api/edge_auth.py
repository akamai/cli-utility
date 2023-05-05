from __future__ import annotations

from pathlib import Path

import requests
from akamai.edgegrid import EdgeGridAuth
from akamai.edgegrid import EdgeRc
from utils.exceptions import setup_logger

logger = setup_logger()


class AkamaiSession:
    def __init__(self):
        home = str(Path.home())
        edgerc_file = EdgeRc(f'{home}/.edgerc')
        self.host = edgerc_file.get('default', 'host')
        self.base_url = f'https://{self.host}'
        self.session = requests.Session()
        self.session.auth = EdgeGridAuth.from_edgerc(edgerc_file, 'default')
        self.account_switch_key = None

    @property
    def params(self) -> dict:
        return {'accountSwitchKey': self.account_switch_key} if self.account_switch_key else {}

    def form_url(self, url: str, account_key: str) -> str:
        account_switch_key = f'&accountSwitchKey={account_key}' if account_key is not None else ''
        if '?' in url:
            return f'{url}{account_switch_key}'
        else:
            account_switch_key = account_switch_key.translate(account_switch_key.maketrans('&', '?'))
            return f'{url}{account_switch_key}'

    def update_account_key(self, account_key: str) -> None:
        self.account_switch_key = account_key


if __name__ == '__main__':
    pass
