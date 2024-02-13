# Techdocs reference
# https://techdocs.akamai.com/cp-codes/reference/get-cpcode
from __future__ import annotations

import logging

from akamai_api.edge_auth import AkamaiSession


class CpCode(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 logger: logging.Logger = None):
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
        self.MODULE = f'{self.base_url}/cprg/v1'
        self.headers = {'Accept': 'application/json'}
        self._params = super().params
        self.logger = logger

    def list_cpcode(self) -> tuple:
        return self.session.get(f'{self.MODULE}/cpcodes', params=self._params, headers=self.headers)

    def get_cpcode(self, cpcode: str) -> tuple:
        return self.session.get(f'{self.MODULE}/cpcodes/{cpcode}', params=self._params, headers=self.headers)

    def create_reporting_group(self, payload: dict) -> dict:
        url = f'{self.MODULE}/reporting-groups'
        resp = self.session.post(url, headers=self.headers, params=self._params, json=payload)
        return resp

    def list_reporting_group(self) -> dict:
        url = f'{self.MODULE}/reporting-groups'
        resp = self.session.get(url, headers=self.headers, params=self._params)
        return resp

    def list_product_of_reporting_group(self, reporting_group_id: int) -> dict:
        url = f'{self.MODULE}/reporting-groups/{reporting_group_id}/products'
        resp = self.session.get(url, headers=self.headers, params=self._params)
        return resp

    def get_reporting_group(self, reporting_group_id: int) -> dict:
        url = f'{self.MODULE}/reporting-groups/{reporting_group_id}'
        resp = self.session.get(url, headers=self.headers, params=self._params)
        return resp

    def delete_reporting_group(self, reporting_group_id: int) -> dict:
        url = f'{self.MODULE}/reporting-groups/{reporting_group_id}'
        resp = self.session.delete(url, headers=self.headers, params=self._params)
        return resp


if __name__ == '__main__':
    pass
