from __future__ import annotations

from collections import defaultdict
from urllib.parse import urlparse

from akamai_api.edge_auth import AkamaiSession
from boltons.iterutils import remap
from rich import print_json
from utils import files
from utils._logging import setup_logger


logger = setup_logger()


class Papi(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None):
        super().__init__()
        self.MODULE = f'{self.base_url}/papi/v1'
        self.headers = {'PAPI-Use-Prefixes': 'false',
                        'Accept': 'application/json',
                        'Content-Type': 'application/json'}
        self.contract_id = self.contract_id
        self.group_id = self.group_id
        self.account_switch_key = account_switch_key
        self.property_id = None

    def get_contracts(self) -> list:
        response = self.session.get(f'{self.MODULE}/contracts', params=self.params, headers=self.headers)
        logger.warning(f'Collecting contracts {urlparse(response.url).path:<30} {response.status_code}')
        if response.status_code == 200:
            return response.json()['contracts']['items']
        else:
            return response.json()

    def get_groups(self) -> list:
        response = self.session.get(f'{self.MODULE}/groups', params=self.params, headers=self.headers)
        if response.status_code == 200:
            return response.json()['groups']['items']
        else:
            logger.info(print_json(data=response.json()))
            return response.json()

    def get_properties_per_group(self, group_id: int, contract_id: str) -> list:
        url = self.form_url(f'{self.MODULE}/properties?contractId={contract_id}&groupId={group_id}')
        response = self.session.get(url, headers=self.headers)
        logger.debug(f'Collecting properties {urlparse(response.url).path:<30} {response.status_code} {response.url}')
        if response.status_code == 200:
            return response.json()['properties']['items']
        else:
            return response.json()

    def get_properties_detail(self, property_id: int, version: int) -> list:
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}')
        response = self.session.get(url)
        logger.debug(f'Collecting properties version detail {urlparse(response.url).path:<30} {response.status_code}')
        if property_id == 303315:
            logger.critical(f'{property_id=}')
            files.write_json('output/response_version_detail.json', response.json())
        if response.status_code == 200:
            return response.json()['versions']['items']
        else:
            return response.json()

    def get_properties_ruletree_digest(self, property_id: int, version: int) -> list:
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}/rules')
        response = self.session.get(url)
        logger.debug(f'Collecting ruletree digest {urlparse(response.url).path:<30} {response.status_code}')
        if property_id == 303315:
            logger.critical(f'{property_id=}')
            files.write_json('output/response_complete_ruletree_digest.json', response.json())
        if response.status_code == 200:
            return response.json()['ruleFormat']
        else:
            return response.json()

    def get_property_hostnames(self, property_id: int) -> list:
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/hostnames')
        response = self.session.get(url, headers=self.headers)
        logger.debug(f'Collecting hostname for a property {urlparse(response.url).path:<30} {response.status_code} {response.url}')
        if response.status_code == 200:
            return response.json()['hostnames']['items']
        else:
            return response.json()

    def search_property_by_name(self, property_name: str) -> tuple:
        url = self.form_url(f'{self.MODULE}/search/find-by-value')
        payload = {'propertyName': property_name}
        response = self.session.post(url, json=payload, headers=self.headers)

        if response.status_code == 200:
            print('\n\n')
            logger.info(f'Found property {property_name}')
            try:
                property_items = response.json()['versions']['items']
            except:
                logger.info(print_json(response.json()))
            self.contract_id = property_items[0]['contractId']
            self.group_id = int(property_items[0]['groupId'])
            self.property_id = int(property_items[0]['propertyId'])
            return 200, property_items
        else:
            logger.debug(print_json(response.json()))
            return response.status_code, response.json()

    def search_property_by_hostname(self, hostname: str) -> tuple:
        url = self.form_url(f'{self.MODULE}/search/find-by-value')
        payload = {'hostname': hostname}
        response = self.session.post(url, json=payload, headers=self.headers)
        if response.status_code == 200:
            if hostname == 'Others':
                logger.critical(f'{hostname=}')
                files.write_json('output/error_others.json', response.json())
                return 'ERROR-Others'

            try:
                property_items = response.json()['versions']['items'][0]
                return property_items['propertyName']
            except:
                if hostname == 'minor-qa-www.samsclub.com':
                    logger.critical(f'{hostname=}')
                    files.write_json('output/error_x.json', response.json())
                return 'ERROR-X'
        else:
            files.write_json(f'output/error_{response.status_code }.json', response.json())
            return 'ERROR'

    def property_version(self, items: dict) -> tuple:
        prd_version = 0
        stg_version = 0

        for item in items:
            if item['stagingStatus'] == 'ACTIVE':
                stg_version = item['propertyVersion']
            if item['productionStatus'] == 'ACTIVE':
                prd_version = item['propertyVersion']

        if len(items) == 1:
            stg_version = items['propertyVersion']
            prd_version = stg_version
        else:
            dd = defaultdict(list)
            for d in items:
                for k, v in d.items():
                    dd[k].append(v)
            if stg_version == 0:
                stg_version = max(dd['propertyVersion'])
            if prd_version == 0:
                prd_version = max(dd['propertyVersion'])

        logger.info(f'Found staging version {stg_version}, production version {prd_version}')
        return stg_version, prd_version

    def property_ruletree(self, property_id: str, version: int, remove_tags: list | None = None):
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}/rules')
        params = {'contractId': self.contract_id,
                  'groupId': self.group_id,
                  'validateRules': 'true',
                  'validateMode': 'full',
                 }
        resp = self.session.get(url, headers=self.headers, params=params)

        # tags we are not interested to compare
        ignore_keys = ['etag', 'errors', 'warnings', 'ruleFormat', 'comments',
                       'accountId', 'contractId', 'groupId',
                       'propertyId', 'propertyName', 'propertyVersion']
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

    def get_ruleformat_schema(self, product_id: str, format_version: str):
        url = self.form_url(f'{self.MODULE}/schemas/products/{product_id}/{format_version}')
        logger.debug(url)
        resp = self.session.get(url, headers=self.headers)
        return resp.status_code, resp.json()

    def list_ruleformat(self):
        url = self.form_url(f'{self.MODULE}/rule-formats')
        logger.debug(url)
        resp = self.session.get(url, headers=self.headers)
        if resp.status_code == 200:
            return resp.status_code, resp.json()['ruleFormats']['items']
        else:
            return resp.status_code, resp.json()


if __name__ == '__main__':
    pass
