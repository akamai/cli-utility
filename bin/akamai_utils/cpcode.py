from __future__ import annotations

import logging

from akamai_api.cpcode import CpCode


class CpCodeWrapper(CpCode):
    def __init__(self,
                 account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 logger: logging.Logger = None):  # type: ignore
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
        self.account_switch_key = account_switch_key
        self.logger = logger

    def list_cpcode(self,
                    contract_id: str | None = None,
                    group_id: str | None = None,
                    product_id: str | None = None,
                    cpcode_name: str | None = None):
        resp = super().list_cpcode(contract_id, group_id, product_id, cpcode_name)
        if resp.ok:
            return resp.json()

    def get_cpcode_name(self, cpcode: int) -> dict:
        resp = super().get_cpcode(cpcode)
        if resp.ok:
            return resp.json()['cpcodeName']


if __name__ == '__main__':
    pass
