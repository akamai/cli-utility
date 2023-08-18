# Techdocs reference
# https://techdocs.akamai.com/property-mgr/reference/api-summary
from __future__ import annotations

import logging
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from urllib.parse import urlparse

from akamai_api.edge_auth import AkamaiSession
from boltons.iterutils import remap
from requests.structures import CaseInsensitiveDict
from rich import print_json
from utils import _logging as lg
from utils import files


class Papi(AkamaiSession):
    def __init__(self,
                 account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 cookies: str | None = None,
                 logger: logging.Logger = None):
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc, cookies=cookies)

        self.MODULE = f'{self.base_url}/papi/v1'
        self.headers = {'PAPI-Use-Prefixes': 'false',
                        'Accept': 'application/json',
                        'Content-Type': 'application/json'}
        self.contract_id = None
        self.group_id = None
        self.account_switch_key = account_switch_key if account_switch_key else None
        self.account_id = None
        self.property_id = None
        self.property_name = None
        self.asset_id = None
        self.cookies = self.cookies
        self.logger = logger

    # BUILD/CATALOG
    def get_build_detail(self):
        resp = self.session.get(f'{self.MODULE}/build')
        if resp.status_code == 200:
            return resp.json()

    def get_products(self) -> list:
        response = self.session.get(f'{self.MODULE}/products', params=self.params, headers=self.headers)
        self.logger.warning(f'Collecting contracts {urlparse(response.url).path:<30} {response.status_code}')
        if response.status_code == 200:
            return response.json()['products']['items']
        else:
            return response.json()

    def get_account_id(self) -> list:
        response = self.session.get(f'{self.MODULE}/contracts', params=self.params, headers=self.headers)
        self.logger.debug(f'Retrieving account id {urlparse(response.url).path:<30} {response.status_code}')
        if response.status_code == 200:
            self.account_id = response.json()['accountId']
            return self.account_id
        else:
            return response.json()

    def get_contracts(self) -> list:
        response = self.session.get(f'{self.MODULE}/contracts', params=self.params, headers=self.headers)
        self.logger.debug(f'Collecting contracts {urlparse(response.url).path:<30} {response.status_code}')
        if response.status_code == 200:
            # print_json(data=response.json())
            return response.json()['contracts']['items']
        else:
            return response.json()

    def get_edgehostnames(self, contract_id: str, group_id: int) -> list:
        url = self.form_url(f'{self.MODULE}/edgehostnames?contractId={contract_id}&groupId={group_id}')

        response = self.session.get(url, headers=self.headers)
        self.logger.info(f'Collecting edgehostnames for contract/group {urlparse(response.url).path:<30} {response.status_code} {response.url}')
        if response.status_code == 200:
            return response.json()['edgeHostnames']['items']
        else:
            return response.json()

    def get_account_hostnames(self):
        url = self.form_url(f'{self.MODULE}/hostnames?sort=hostname%3Aa')
        resp = self.session.get(url, headers=self.headers)
        if resp.status_code == 200:
            return resp.json()['hostnames']['items']
        else:
            return resp.json()

    # BULK
    def bulk_search_properties(self):

        url = '/bulk/rules-search-requests'
        if self.contract_id:
            query_params = f'contractId={self.contract_id}'
        if self.group_id:
            query_params = f'?{query_params}&groupId={self.group_id}' if query_params else f'?groupId={self.group_id}'
        url = self.form_url(f'{self.MODULE}{url}{query_params}')

        search_query = {'bulkSearchQuery': {'syntax': 'JSONPATH', 'match': '$.name'}}

        resp = self.session.post(url, json=search_query, headers=self.headers)
        self.logger.info(resp.status_code)
        self.logger.info(print_json(data=resp.json()))

    def bulk_activate_properties(self, emails: list,
                                 contract_id: str,
                                 group_id: int,
                                 network: str,
                                 note: str,
                                 property_id: str,
                                 version: str):

        url = f'{self.MODULE}/bulk/activations'
        params = f'?contractId={contract_id}&groupId={group_id}'
        url = self.form_url(f'{url}{params}')
        payload = {'activatePropertyVersions': [{'network': network.upper(),
                                                 'note': note,
                                                 'propertyId': property_id,
                                                 'propertyVersion': version
                                                }
                                               ],
                   'defaultActivationSettings': {'notifyEmails': emails,
                                                 'acknowledgeAllWarnings': True,
                                                 'fastPush': True,
                                                 'useFastFallback': True
                                                }
                  }
        resp = self.session.post(url, json=payload, headers=self.headers)
        return resp.status_code, resp.json()

    # GROUPS
    def get_groups(self) -> list:
        response = self.session.get(f'{self.MODULE}/groups', params=self.params, headers=self.headers)
        if response.status_code == 200:
            return 200, response.json()['groups']['items']
        elif response.status_code == 401:
            # accountSwitchKey is invalid
            self.logger.error(response.json()['title'])
            return response.status_code, response.json()['title']
        else:
            self.logger.error(f'{response.text}')
            return response.status_code, response.json()

    # SEARCH
    def search_property_by_name(self, property_name: str) -> tuple:
        url = self.form_url(f'{self.MODULE}/search/find-by-value')
        payload = {'propertyName': property_name}
        headers = {'PAPI-Use-Prefixes': 'false',
                   'Accept': 'application/json',
                   'Content-Type': 'application/json'}
        resp = self.session.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            try:
                property_items = resp.json()['versions']['items']
                # print_json(data=property_items)
            except:
                self.logger.info(print_json(resp.json()))
            self.logger.debug(f'{property_name} {resp.status_code} {resp.url} {property_items}')
            if len(property_items) == 0:
                self.logger.debug(f'Not found {property_name}')
                return 400, f'Not found {property_name}'
            else:
                self.account_id = property_items[0]['accountId']
                self.contract_id = property_items[0]['contractId']
                self.asset_id = property_items[0]['assetId']
                self.group_id = int(property_items[0]['groupId'])
                self.property_id = int(property_items[0]['propertyId'])
                return 200, property_items
        elif resp.status_code == 401:
            self.logger.error(resp.json()['title'])
            sys.exit()
        elif 'WAF deny rule IPBLOCK-BURST' in resp.json()['detail']:
            self.logger.error(resp.json()['detail'])
            lg.countdown(540, msg='Oopsie! You just hit rate limit.', logger=self.logger)
            sys.exit()
        else:
            self.logger.info(f'{property_name:<40} {resp.status_code}')
            print_json(data=resp.json())
            return resp.status_code, resp.json()

    def search_property_by_hostname(self, hostname: str) -> tuple:
        url = self.form_url(f'{self.MODULE}/search/find-by-value')
        payload = {'hostname': hostname}
        response = self.session.post(url, json=payload, headers=self.headers)
        if response.status_code == 200:
            if hostname == 'Others':
                self.logger.critical(f'{hostname=}')
                files.write_json('output/error_others.json', response.json())
                return 'ERROR_Others'
            try:

                property_items = response.json()['versions']['items'][0]
                property_name = property_items['propertyName']
                self.logger.debug(property_name)
                # print_json(data=response.json()['versions']['items']))
                return property_items['propertyName']
            except:
                return 'ERROR_X'
        else:
            files.write_json(f'output/error_{response.status_code}.json', response.json())
            return f'ERROR_{response.status_code}'

    # PROPERTIES
    def get_propertyname_per_group(self, group_id: int, contract_id: str) -> list:
        url = self.form_url(f'{self.MODULE}/properties?contractId={contract_id}&groupId={group_id}')
        response = self.session.get(url, headers=self.headers)
        self.logger.debug(f'Collecting properties {urlparse(response.url).path:<30} {response.status_code} {response.url}')
        if response.status_code == 200:
            return response.json()['properties']['items']
        else:
            return response.json()

    def get_property_version_latest(self, property_id: int) -> dict:
        url = self.form_url(f'{self.MODULE}/properties/{property_id}')
        response = self.session.get(url, headers=self.headers)
        self.logger.debug(f'Collecting properties {urlparse(response.url).path:<30} {response.status_code} {response.url}')
        if response.status_code == 200:
            return response.json()['properties']['items'][0]
        else:
            return response.json()

    def get_property_version_full_detail(self, property_id: int, version: int) -> list:
        '''
        sample response
        {
            "propertyId": "prp_303315",
            "propertyName": "i-internal.test.com_pm",
            "accountId": "act_1-1IY5Z",
            "contractId": "ctr_3-M4N4PX",
            "groupId": "grp_14788",
            "assetId": "aid_10395498",
            "versions": {
                "items": [ {"propertyVersion": 4,
                            "updatedByUser": "test@akamai.com",
                            "updatedDate": "2020-06-30T06:40:54Z",
                            "productionStatus": "ACTIVE",
                            "stagingStatus": "ACTIVE",
                            "etag": "62682b5e5b57f282ef7e927ecbfe97d6f3f9d355",
                            "productId": "prd_SPM",
                            "ruleFormat": "latest",
                            "note": "test note"
                            }
                          ]
                        }
        }
        '''
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}')
        response = self.session.get(url)
        self.logger.debug(f'Collecting properties version detail {urlparse(response.url).path:<30} {response.status_code}')
        if response.status_code == 200:
            return response.json()
        else:
            return response.json()

    def get_property_version_detail(self, property_id: int, version: int) -> list:
        '''
        sample response
        {
            "propertyId": "prp_303315",
            "propertyName": "i-internal.test.com_pm",
            "accountId": "act_1-1IY5Z",
            "contractId": "ctr_3-M4N4PX",
            "groupId": "grp_14788",
            "assetId": "aid_10395498",
            "versions": {
                "items": [ {"propertyVersion": 4,
                            "updatedByUser": "test@akamai.com",
                            "updatedDate": "2020-06-30T06:40:54Z",
                            "productionStatus": "ACTIVE",
                            "stagingStatus": "ACTIVE",
                            "etag": "62682b5e5b57f282ef7e927ecbfe97d6f3f9d355",
                            "productId": "prd_SPM",
                            "ruleFormat": "latest",
                            "note": "test note"
                            }
                          ]
                        }
        }
        '''
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}')
        response = self.session.get(url)
        self.logger.debug(f'Collecting properties version detail {urlparse(response.url).path:<30} {response.status_code}')
        if response.status_code == 200:
            propertyName = response.json()['propertyName']
            assetId = response.json()['assetId'][4:]
            gid = response.json()['groupId'][4:]
            acc_url = f'https://control.akamai.com/apps/property-manager/#/property-version/{assetId}/{version}/edit?gid={gid}'
            self.logger.debug(f'{propertyName:<46} {acc_url}')
            return response.json()
        else:
            return response.json()

    def get_properties_version_metadata_xml(self,
                                            property_name: str,
                                            asset_id: int,
                                            group_id: int,
                                            version: int,
                                            ignore_tags: list | None = None) -> str:

        url = 'https://control.akamai.com/pm-backend-blue/service/v1/properties/version/metadata'
        if self.account_switch_key:
            qry = f'?aid={asset_id}&gid={group_id}&v={version}&type=pm&dl=true&accountId={self.account_switch_key}'
        else:
            account_id = self.get_account_id()
            qry = f'?aid={asset_id}&gid={group_id}&v={version}&type=pm&dl=true&accountId={account_id}'
        url = f'{url}{qry}'

        self.headers['X-Xsrf-Token'] = self.cookies['XSRF-TOKEN']
        self.headers['Cookie'] = f"AKASSO={self.cookies['AKASSO']}; XSRF-TOKEN={self.cookies['XSRF-TOKEN']}; AKATOKEN={self.cookies['AKATOKEN']};"

        if 'Accept' in self.headers.keys():
            del self.headers['Accept']  # because we get XML content

        response = self.session.get(url, headers=self.headers)
        if response.status_code == 200:
            filepath = f'output/0_diff/xml/{property_name}_v{version}.xml'
            with open(filepath, 'wb') as file:
                file.write(response.content)
            files.remove_tags_from_xml_file(filepath, ignore_tags)

        elif response.status_code in [400, 401]:
            msg = response.json()['title']
        elif response.status_code == 403:
            msg = response.json()['errors'][0]['detail']
        else:
            msg = response.json()

        try:
            return filepath
        except:
            s = response.status_code
            t = response.text
            u = response.url
            z = response.content
            self.logger.error(f'{s} [{msg}] {u}')
            # logger.debug(print_json(data=self.headers))
            # print_json(data=self.cookies)
            sys.exit()

    def get_properties_ruletree_digest(self, property_id: int, version: int) -> list:
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}/rules')
        response = self.session.get(url)
        self.logger.debug(f'Collecting ruletree digest {urlparse(response.url).path:<30} {response.status_code}')
        if property_id == 303315:
            self.logger.critical(f'{property_id=}')
            files.write_json('output/response_complete_ruletree_digest.json', response.json())
        if response.status_code == 200:
            return response.json()['ruleFormat']
        else:
            return response.json()

    def update_property_ruletree(self, property_id: int, version: int, payload: dict) -> tuple:
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}/rules')
        headers = {'PAPI-Use-Prefixes': 'false',
                   'Accept': 'application/json',
                   'content-type': 'application/json-patch+json'}
        resp = self.session.patch(url, json=payload, headers=headers)
        return resp.status_code, resp.text

    def get_property_hostnames(self, property_id: int) -> list:
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/hostnames')
        response = self.session.get(url, headers=self.headers)
        self.logger.debug(f'Collecting hostname for a property {urlparse(response.url).path:<30} {response.status_code} {response.url}')
        if response.status_code == 200:
            return response.json()['hostnames']['items']
        else:
            return response.json()

    def get_property_version_hostnames(self, property_id: int, version: int) -> list:
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}/hostnames?includeCertStatus=true')
        resp = self.session.get(url, headers=self.headers)
        self.logger.debug(resp.url)
        self.logger.debug(f'Collecting hostname for a property {urlparse(resp.url).path:<30} {resp.status_code} {resp.url}')
        if resp.status_code == 200:
            self.property_name = resp.json()['propertyName']
            # print_json(data=resp.json())
            return resp.json()['hostnames']['items']
        else:
            return resp.json()

    def property_version(self, items: dict) -> tuple:
        prd_version = 0
        stg_version = 0
        for item in items:
            if item['stagingStatus'] == 'ACTIVE':
                stg_version = item['propertyVersion']
            if item['productionStatus'] == 'ACTIVE':
                prd_version = item['propertyVersion']

        if len(items) == 1:
            stg_version = items[0]['propertyVersion']
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
        self.logger.info(f'{items[0]["propertyName"]:<40} staging:production    v{stg_version}:v{prd_version}')
        return stg_version, prd_version

    def property_rate_limiting(self, property_id: int, version: int):
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}/rules')
        resp = self.session.get(url, headers=self.headers)
        self.headers['Accept'] = 'application/vnd.akamai.papirules.latest+json'

        ruletree_response = self.session.get(url, headers=self.headers)
        self.logger.debug(f'{urlparse(resp.url).path:<30} {resp.status_code}')
        if resp.status_code == 200:

            self.property_name = ruletree_response.json()['propertyName']
            # https://github.com/psf/requests/issues/1380
            # rate_limiting_dict = json.loads(response.headers)
            return resp.headers, ruletree_response.json()

    # RULETREE
    def property_ruletree(self, property_id: int, version: int, remove_tags: list | None = None):
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}/rules')
        try:
            self.contract_id = self.get_property_version_full_detail(property_id, version, 'contractId')
        except:
            try:
                self.contract_id = self.get_property_version_full_detail(property_id, version)['contractId'][4:]
            except:
                version_json = self.get_property_version_detail(property_id, version)
                if version_json['status'] == 404:
                    return version_json['status'], version_json
        try:
            self.group_id = self.get_property_version_full_detail(property_id, version, 'groupId')
        except:
            self.group_id = self.get_property_version_full_detail(property_id, version)['groupId'][4:]

        self.logger.debug(f'{self.contract_id=} {self.group_id=}')

        params = {'contractId': self.contract_id,
                  'groupId': self.group_id,
                  'validateRules': 'true',
                  'validateMode': 'full',
                 }

        resp = self.session.get(url, headers=self.headers, params=params)

        if resp.status_code == 200:
            # tags we are not interested to compare
            self.property_name = resp.json()['propertyName']
            ignore_keys = ['etag', 'errors', 'warnings', 'ruleFormat', 'comments',
                           'accountId', 'contractId', 'groupId',
                           'propertyId', 'propertyName', 'propertyVersion']
            if remove_tags is not None:
                addl_keys = [tag for tag in remove_tags]
                if addl_keys is not None:
                    ignore_keys = ignore_keys + addl_keys
            self.logger.debug(f'{ignore_keys}')
            mod_resp = remap(resp.json(), lambda p, k, v: k not in ignore_keys)
            self.logger.debug(params)
            self.logger.debug(resp.status_code)
            self.logger.debug(mod_resp)
            return 200, mod_resp
        else:
            self.logger.error(f'{resp.status_code} {self.contract_id=} {self.group_id=} {resp.url}')
            return resp.status_code, resp.json()

    def get_property_full_ruletree(self, property_id: int, version: int):
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}/rules')
        params = {'contractId': self.contract_id,
                  'groupId': self.group_id}
        resp = self.session.get(url, headers=self.headers, params=params)
        self.logger.debug(f'{resp.status_code} {resp.text}')
        return resp

    def get_ruleformat_schema(self, product_id: str, format_version: str | None = 'latest'):
        url = self.form_url(f'{self.MODULE}/schemas/products/{product_id}/{format_version}')
        self.logger.debug(url)
        resp = self.session.get(url, headers=self.headers)
        return resp.status_code, resp.json()

    def list_ruleformat(self):
        url = self.form_url(f'{self.MODULE}/rule-formats')
        self.logger.debug(url)
        resp = self.session.get(url, headers=self.headers)
        if resp.status_code == 200:
            return resp.status_code, resp.json()['ruleFormats']['items']
        else:
            return resp.status_code, resp.json()

    # ACTIVATION
    def activate_property_version(self, property_id: int, version: int,
                                  network: str,
                                  note: list,
                                  emails: list):

        url = self.form_url(f'{self.MODULE}/properties/{property_id}/activations')
        payload = {'acknowledgeAllWarnings': True,
                   'activationType': 'ACTIVATE',
                   'ignoreHttpErrors': True,
                   'complianceRecord': {'noncomplianceReason': 'EMERGENCY',
                                        'peerReviewedBy': 'anpatel@akamai.com',
                                        'unitTested': True,
                                        'ticketId': note[1]
                                       }
                    }
        payload['network'] = network.upper()
        payload['propertyVersion'] = version
        payload['note'] = note[0]
        payload['notifyEmails'] = emails
        self.logger.debug(payload)
        self.logger.debug(self.headers)

        resp = self.session.post(url, json=payload, headers=self.headers)
        self.logger.debug(resp.url)
        if resp.status_code == 201:
            return resp.status_code, resp.json()['activationLink']
        elif resp.status_code == 422:  # pending activation or deactivation.
            self.logger.critical(resp.json()['detail'])
            return resp.status_code, resp.json()['detail']
        elif resp.status_code == 429:  # Hit rate limit
            return resp.status_code, resp.json()['title']
        else:
            return resp.status_code, resp.json()

    def activation_status(self, property_id: int, activation_id: int):
        url = f'{self.MODULE}/properties/{property_id}/activations/{activation_id}'
        resp = self.session.get(self.form_url(url))
        if resp.status_code == 200:
            return 200, resp.json()['activations']['items']
        else:
            return resp.status_code, resp.json()

    # CUSTOM BEHAVIOR
    def list_custom_behaviors(self):
        url = self.form_url(f'{self.MODULE}/custom-behaviors')
        resp = self.session.get(url, headers=self.headers)
        self.logger.debug(resp.status_code)
        if resp.status_code == 200:
            return resp.status_code, resp.json()['customBehaviors']['items']
        else:
            return resp.status_code, resp.json()

    def get_custom_behaviors(self, id: str):
        url = self.form_url(f'{self.MODULE}/custom-behaviors/{id}')
        resp = self.session.get(url, headers=self.headers)
        # print_json(data=resp.json())
        if resp.status_code == 200:
            return resp.status_code, resp.json()['customBehaviors']['items'][0]['xml']
        else:
            return resp.status_code, resp.json()


if __name__ == '__main__':
    pass
