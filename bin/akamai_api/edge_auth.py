from __future__ import annotations

from pathlib import Path

import requests
from akamai.edgegrid import EdgeGridAuth
from akamai.edgegrid import EdgeRc
from utils._logging import setup_logger

logger = setup_logger()


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
