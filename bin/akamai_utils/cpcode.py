from __future__ import annotations

from akamai_api.cpcode import CpCode


class CpCodeWrapper(CpCode):
    def __init__(self, account_switch_key: str | None = None,
                 section: str | None = None):

        super().__init__(account_switch_key=account_switch_key)
        self.account_switch_key = account_switch_key

    def list_cpcode(self,
                    contract_id: str | None = None,
                    group_id: str | None = None,
                    product_id: str | None = None,
                    cpcode_name: str | None = None):
        status, json_output = super().list_cpcode(contract_id, group_id, product_id, cpcode_name)
        if status == 200:
            return json_output
        else:
            return ''

    def get_cpcode_name(self, cpcode: int) -> dict:
        status, json_output = super().get_cpcode(cpcode)
        if status == 200:
            return json_output['cpcodeName']
        else:
            return ''


if __name__ == '__main__':
    pass
