# Techdocs reference
# https://techdocs.akamai.com/application-security/reference/api
from __future__ import annotations

import sys

from akamai_api.edge_auth import AkamaiSession
from boltons.iterutils import remap
from rich import print_json
from utils import _logging as lg
from utils import files


logger = lg.setup_logger()


class Appsec(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None, section: str | None = None):
        super().__init__(account_switch_key=account_switch_key, section=section)
        self.MODULE = f'{self.base_url}/appsec/v1'
        self.headers = {'PAPI-Use-Prefixes': 'false',
                        'Accept': 'application/json',
                        'Content-Type': 'application/json'}
        self.contract_id = self.contract_id
        self.group_id = self.group_id
        self.config_id = None
        self.account_switch_key = account_switch_key
        self.cookies = self.cookies

    def list_waf_configs(self):
        url = self.form_url(f'{self.MODULE}/configs')
        response = self.session.get(url, headers=self.headers)
        return response.status_code, response.json()['configurations']

    def get_config_detail(self, config_id: int):
        url = self.form_url(f'{self.MODULE}/configs/{config_id}')
        response = self.session.get(url, headers=self.headers)
        self.config_id = config_id
        return response.status_code, response.json()

    def get_config_version_detail(self, config_id: int, version: int, remove_tags: list | None = None):
        url = self.form_url(f'{self.MODULE}/export/configs/{config_id}/versions/{version}')
        resp = self.session.get(url, headers=self.headers)

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

    def get_config_version_metadata_xml(self,
                                        config_name: str,
                                        version: int,
                                        cookies: str | None = None) -> dict:

        url = f'https://control.akamai.com/appsec-configuration/v1/configs/{self.config_id}/versions/{version}/metadata'

        headers = {}
        if cookies:
            headers['X-Xsrf-Token'] = 'ZTgzMWFjYzEtMjBjNy00NzM3LTlmMmMtNGExYWYzMTRkZDQ2'
            headers['Cookie'] = cookies
        else:
            try:
                headers['X-Xsrf-Token'] = self.cookies['XSRF-TOKEN']
            except KeyError:
                logger.error('missing X-Xsrf-Token')
            try:
                headers['Cookie'] = f"AKASSO={self.cookies['AKASSO']}; XSRF-TOKEN={self.cookies['XSRF-TOKEN']}; AKATOKEN={self.cookies['AKATOKEN']};"
            except KeyError:
                sys.exit(logger.error('invalid Cookie, a combination of AKASSO, XSRF-TOKEN, and XSRF-TOKEN'))

        response = self.session.get(url, headers=headers)
        if response.status_code == 200:
            filepaths = {}
            portalWaf_str = response.json()['portalWaf']

            filepath = f'output/diff/xml/{self.config_id}_{config_name}_portalWaf_v{version}.xml'
            filepaths['portalWaf'] = f'output/diff/xml/{self.config_id}_{config_name}_portalWaf_v{version}.xml'
            with open(filepath, 'w') as f:
                f.write(portalWaf_str)

            wafAfter_str = response.json()['wafAfter']
            filepath = f'output/diff/xml/{self.config_id}_{config_name}_wafAfter_v{version}.xml'
            filepaths['wafAfter'] = f'output/diff/xml/{self.config_id}_{config_name}_wafAfter_v{version}.xml'
            with open(filepath, 'w') as f:
                f.write(wafAfter_str)
        elif response.status_code in [400, 401]:
            msg = response.json()['title']
        elif response.status_code == 403:
            msg = response.json()['detail']
        else:
            msg = response.json()

        try:
            return filepaths
        except:
            s = response.status_code
            t = response.text
            u = response.url
            z = response.content
            logger.error(f'{s} [{msg}] {u}')
            logger.debug(print_json(data=headers))
            logger.debug(print_json(data=self.cookies))
            sys.exit()


if __name__ == '__main__':
    pass
