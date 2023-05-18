from __future__ import annotations

from akamai_api.edge_auth import AkamaiSession
from boltons.iterutils import remap
from utils._logging import setup_logger

logger = setup_logger()


class Appsec(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None):
        super().__init__()
        self.MODULE = f'{self.base_url}/appsec/v1'
        self.headers = {'PAPI-Use-Prefixes': 'false',
                        'Accept': 'application/json',
                        'Content-Type': 'application/json'}
        self.contract_id = self.contract_id
        self.group_id = self.group_id
        self.account_switch_key = account_switch_key
        self.property_id = None

    def list_waf_configs(self):
        url = self.form_url(f'{self.MODULE}/configs')
        response = self.session.get(url, headers=self.headers)
        return response.status_code, response.json()['configurations']

    def get_config_detail(self, config_id: int):
        url = self.form_url(f'{self.MODULE}/configs/{config_id}')
        response = self.session.get(url, headers=self.headers)
        logger.debug(response.json())
        return response.status_code, response.json()

    def get_config_version_detail(self, config_id: int, version: int, remove_tags: list | None = None):
        url = self.form_url(f'{self.MODULE}/export/configs/{config_id}/versions/{version}')
        resp = self.session.get(url, headers=self.headers)
        logger.info(resp.url)

        # tags we are not interested to compare
        ignore_keys = ['createDate', 'updateDate', 'time']
        if remove_tags is not None:
            addl_keys = [tag for tag in remove_tags]
            if addl_keys is not None:
                ignore_keys = ignore_keys + addl_keys
        logger.debug(f'{ignore_keys}')

        if resp.status_code == 200:
            mod_resp = remap(resp.json(), lambda p, k, v: k not in ignore_keys)
            return 200, mod_resp
        else:
            return resp.status_code, resp.json()


if __name__ == '__main__':
    pass
