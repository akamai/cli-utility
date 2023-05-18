from __future__ import annotations

from akamai_api.papi import Appsec
from utils._logging import setup_logger

logger = setup_logger()


class AppsecWrapper(Appsec):
    def __init__(self, account_switch_key: str | None = None):
        super().__init__()
        self.account_switch_key = account_switch_key


if __name__ == '__main__':
    pass
