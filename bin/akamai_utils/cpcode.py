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
        print()

    def list_cpcode(self):
        resp = super().list_cpcode()
        if not resp.ok:
            self.logger.error(resp.json())
        else:
            return resp.json()

    def get_cpcode_name(self, cpcode: int) -> dict:
        resp = super().get_cpcode(cpcode)
        if not resp.ok:
            self.logger.error(resp.json())
        else:
            return resp.json()['cpcodeName']

    def create_reporting_group(self, payload: dict) -> dict:
        return super().create_reporting_group(payload)

    def list_reporting_group(self) -> dict:
        return super().list_reporting_group()

    def list_product_of_reporting_group(self, reporting_group_id: int) -> dict:
        return super().list_product_of_reporting_group(reporting_group_id)

    def get_reporting_group(self, reporting_group_id: int) -> dict:
        return super().get_reporting_group(reporting_group_id)

    def delete_reporting_group(self, reporting_group_id: int) -> dict:
        return super().delete_reporting_group(reporting_group_id)


if __name__ == '__main__':
    pass
