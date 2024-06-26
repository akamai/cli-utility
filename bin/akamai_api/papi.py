# Techdocs reference
# https://techdocs.akamai.com/property-mgr/reference/api-summary
from __future__ import annotations

import logging
import sys
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from urllib.parse import urlparse

from akamai_api.edge_auth import AkamaiSession
from boltons.iterutils import remap
from jsonpath_ng import parse
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
        self.version = 0
        self.staging_version = 0
        self.latest_version = 0
        self.product_id = None
        self.asset_id = None
        self.cookies = self.cookies
        self.logger = logger

    def create_new_property_version(self, property_id: str, base_version: int) -> int:
        payload = {'createFromVersion': base_version}
        url = f'{self.MODULE}/properties/{property_id}/versions'

        resp = self.session.post(url, headers=self.headers,
                                      params=self.params,
                                      json=payload)
        if resp.ok:
            new_version = resp.json()['versionLink'].split('?')[0].split('/')[-1]
            return int(new_version)

        return 0

    def add_shared_ehn(self, property_id: str, version: int, ehn: str | None = 'whateverlumen-com.akamaized.net'):
        url = f'{self.MODULE}/properties/{property_id}/versions/{version}/hostnames'

        if ehn.endswith('.akamaized.net'):
            cnameFrom = ehn.split('.akamaized.net')[0]
            payload = {'add': [{
                                'cnameFrom': ehn,
                                'cnameTo': ehn,
                               }]
                      }
            resp = self.session.patch(url, json=payload, params=self.params, headers=self.headers)
            if not resp.ok:
                print_json(data=resp.json())
        else:
            self.logger.error(f'Incorrect {ehn}')

        return resp

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
    def build_query_params(self) -> dict:
        query_params = {}
        if self.contract_id:
            query_params['contractId'] = self.contract_id
        if self.group_id:
            query_params['groupId'] = self.group_id
        return query_params

    def list_bulk_search(self, id: int):
        url = self.form_url(f'{self.MODULE}/bulk/rules-search-requests/{id}')
        resp = self.session.get(url, params=self.build_query_params(), headers=self.headers)
        return resp

    def list_bulk_patch(self, id: int):
        url = self.form_url(f'{self.MODULE}/bulk/rules-patch-requests/{id}')
        resp = self.session.get(url, params=self.build_query_params(), headers=self.headers)
        return resp

    def list_bulk_activation(self, id: int):
        url = self.form_url(f'{self.MODULE}/bulk/activations/{id}')
        resp = self.session.get(url, params=self.build_query_params(), headers=self.headers)
        return resp

    def list_bulk_create(self, id: int):
        url = self.form_url(f'{self.MODULE}/bulk/property-version-creations/{id}')
        resp = self.session.get(url, params=self.build_query_params(), headers=self.headers)
        return resp

    def bulk_search_properties(self, query: dict):
        url = '/bulk/rules-search-requests'
        url = self.form_url(f'{self.MODULE}{url}')
        resp = self.session.post(url, json=query, params=self.build_query_params(), headers=self.headers)
        return resp

    def bulk_update_behavior(self, properties: list[str, int, list[str]], patch_json: dict) -> list:
        url = self.form_url(f'{self.MODULE}/bulk/rules-patch-requests')
        all_properties = []
        for property in properties:
            prop = {}
            prop['propertyId'] = f'prp_{property[0]}'
            prop['propertyVersion'] = property[1]

            for each_path in property[2]:
                # Create a new patch dictionary for each property
                elements = []
                for patch in patch_json['patches']:
                    new_patch = patch.copy()
                    # new_patch['path'] = f'{each_path}'
                    elements.append(new_patch)
                    prop['patches'] = elements
            all_properties.append(prop)

        payload = {}
        payload['patchPropertyVersions'] = all_properties
        self.logger.debug(all_properties)
        # print_json(data=payload)
        resp = self.session.post(url, json=payload, params=self.build_query_params(), headers=self.headers)
        return resp

    def bulk_delete_add_behavior(self, properties: list[str, int, list[str]]) -> list:
        url = self.form_url(f'{self.MODULE}/bulk/rules-patch-requests')
        all_del_properties = []
        all_add_properties = []
        for property in properties:
            prop = {}
            prop['propertyName'] = property[0]
            prop['propertyId'] = f'prp_{property[1]}'
            prop['propertyVersion'] = property[2]
            self.logger.warning(prop['propertyName'])
            add_prop = prop.copy()
            del_prop = prop.copy()

            rule_resp = self.get_property_full_ruletree(prop['propertyId'], prop['propertyVersion'])
            if not rule_resp.ok:
                continue
            else:
                property_rule = rule_resp.json()['rules']

            elements_del = []
            elements_add = []
            for each_path in property[3]:
                patch_del = {}
                patch_add = {}

                if each_path.endswith('/options'):
                    behavior = each_path.replace('/options', '', 1)
                    jsonpath = files.transform_to_jsonpath(behavior)
                    jsonpath_expr = parse(jsonpath)
                    matches = jsonpath_expr.find({'rules': property_rule})
                    if len(matches) == 0:
                        continue
                    rule = matches[0].value

                    if rule['name'] == 'origin':
                        ops = rule['options']

                        if 'useUniqueCacheKey' in ops.keys():
                            elements_del.append({'op': 'remove',
                                                'path': f'{each_path}/useUniqueCacheKey'
                                                })

                        if 'ipVersion' not in ops.keys():
                            elements_del.append({'op': 'add',
                                                'path': f'{each_path}/ipVersion',
                                                'value': 'IPV4'
                                                })

                        if 'minTlsVersion' in ops.keys():
                            elements_del.append({'op': 'replace',
                                                    'path': f'{each_path}/minTlsVersion',
                                                    'value': 'DYNAMIC'
                                                    })
                        else:
                            elements_del.append({'op': 'add',
                                                    'path': f'{each_path}/minTlsVersion',
                                                    'value': 'DYNAMIC'
                                                    })

                        if 'tlsVersionTitle' not in ops.keys():
                            elements_del.append({'op': 'add',
                                                'path': f'{each_path}/tlsVersionTitle',
                                                'value': ''
                                                })

                    else:
                        patch_del['op'] = 'replace'
                        patch_del['path'] = behavior

                        ops = rule['options']
                        ops['enhancedRfcSupport'] = True
                        ops['honorPrivate'] = True
                        ops['honorMustRevalidate'] = True
                        ops['cacheabilitySettings'] = ''
                        ops['honorNoStore'] = True
                        ops['honorNoCache'] = True
                        ops['expirationSettings'] = ''
                        ops['honorMaxAge'] = True
                        ops['honorSMaxage'] = False
                        ops['revalidationSettings'] = ''
                        ops['honorProxyRevalidate'] = True
                        ops['cacheControlDirectives'] = ''

                        patch_del['value'] = rule
                        elements_del.append(patch_del)
                        print()

            del_prop['patches'] = elements_del
            all_del_properties.append(del_prop)

            add_prop['patches'] = elements_add
            all_add_properties.append(add_prop)

        payload = {}
        payload['patchPropertyVersions'] = all_del_properties
        resp = self.session.post(url, json=payload, params=self.build_query_params(), headers=self.headers)
        self.logger.debug(f'{resp=}')
        time.sleep(10)
        return resp

    def bulk_create_properties(self, properties: list[str, int]):
        url = '/bulk/property-version-creations'
        url = self.form_url(f'{self.MODULE}{url}')
        property_list = []
        for each_property in properties:
            property = {}
            property['propertyId'] = f'prp_{each_property[0]}'
            property['createFromVersion'] = each_property[1]
            property_list.append(property)
        self.logger.critical(property_list)
        payload = {'createPropertyVersions': property_list}
        resp = self.session.post(url, json=payload, params=self.build_query_params(), headers=self.headers)
        self.logger.debug(resp.url)
        self.logger.debug(self.build_query_params())
        # print_json(data=payload)
        # print_json(data=self.headers)
        return resp

    def bulk_activate_properties(self, network: str, email: list, pr_email: str, note: str, properties: list):
        url = self.form_url(f'{self.MODULE}/bulk/activations')
        payload = {}
        property_list = []
        for property in properties:
            prop = {}
            prop['propertyId'] = f'prp_{property[0]}'
            prop['propertyVersion'] = property[1]
            prop['network'] = network.upper()
            prop['note'] = note
            prop['acknowledgeAllWarnings']: True
            prop['fastPush'] = True
            prop['ignoreHttpErrors']: True
            property_list.append(prop)
        payload['activatePropertyVersions'] = property_list
        payload['defaultActivationSettings'] = {'notifyEmails': email,
                                                'acknowledgeAllWarnings': True}
        if network == 'production':
            payload['defaultActivationSettings']['complianceRecord'] = {
                'nonComplianceReason': 'EMERGENCY',
                'peerReviewedBy': pr_email,
                'customerEmail': email[0],
                'unitTested': True
            }
        self.logger.debug(print_json(data=payload))
        resp = self.session.post(url, json=payload, params=self.build_query_params(), headers=self.headers)
        return resp

    def bulk_add_rule(self, properties: list[str, int], patch_json: dict):
        url = '/bulk/rules-patch-requests'
        url = self.form_url(f'{self.MODULE}{url}')
        all_properties = []
        for property in properties:
            prop = {}
            prop['propertyId'] = f'prp_{property[0]}'
            prop['propertyVersion'] = property[1]
            prop['patches'] = patch_json
            all_properties.append(prop)
        payload = {'patchPropertyVersions': all_properties}
        self.logger.warning(payload)
        resp = self.session.post(url, json=payload, params=self.build_query_params(), headers=self.headers)
        return resp

    # GROUPS
    def get_groups(self) -> tuple:
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
        if resp.ok:
            try:
                property_items = resp.json()['versions']['items']
            except:
                self.logger.info(print_json(resp.json()))
            self.logger.debug(f'{property_name} {resp.status_code} {resp.url} {property_items}')
            if len(property_items) == 0:
                self.logger.debug(f'Not found {property_name}')
                return resp.status_code, property_name
            else:
                self.account_id = property_items[0]['accountId']
                self.contract_id = property_items[0]['contractId']
                self.asset_id = property_items[0]['assetId']
                self.group_id = int(property_items[0]['groupId'])
                self.property_id = int(property_items[0]['propertyId'])
                try:
                    self.staging_version = [x['propertyVersion'] for x in property_items
                                            if x['stagingStatus'] == 'ACTIVE'][0]
                except (IndexError, KeyError):
                    self.staging_version = 0

                try:
                    self.production_version = [x['propertyVersion'] for x in property_items
                                               if x['productionStatus'] == 'ACTIVE'][0]
                except (IndexError, KeyError):
                    self.production_version = 0

                self.logger.debug(f'{property_name} {self.staging_version} {self.production_version}')

                return resp.status_code, resp.json()
        elif resp.status_code == 401:
            self.logger.error(resp['title'])
            sys.exit()
        elif 'WAF deny rule IPBLOCK-BURST' in resp.json()['detail']:
            self.logger.error(resp['detail'])
            lg.countdown(540, msg='Oopsie! You just hit rate limit.', logger=self.logger)
            sys.exit()
        else:
            self.logger.debug(f'{property_name:<40} {resp.status_code}')
            return resp.status_code, resp.json()

    def search_property_by_hostname(self, hostname: str) -> tuple:
        url = self.form_url(f'{self.MODULE}/search/find-by-value')
        payload = {'hostname': hostname}
        response = self.session.post(url, json=payload, headers=self.headers)
        if response.ok:
            if hostname == 'Others':
                self.logger.critical(f'{hostname=}')
                files.write_json('output/error_others.json', response.json())
                return 'ERROR_Others'

            try:
                property_items = response.json()['versions']['items'][0]
                property_name = property_items['propertyName']
                self.logger.debug(f'{property_name} {response.url}')
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
        if response.ok:
            items = response.json()['properties']['items']
            self.logger.debug(f'{group_id} {len(items)}')
            # print_json(data=items)
            return items
        else:
            return response.json()

    def get_property_version_latest(self, property_id: int) -> dict:
        url = self.form_url(f'{self.MODULE}/properties/{property_id}')
        response = self.session.get(url, headers=self.headers)
        self.logger.debug(f'Collecting properties {urlparse(response.url).path:<30} {response.status_code} {response.url}')

        if response.ok:
            property = response.json()['properties']['items'][0]
            self.latest_version = int(property.get('latestVersion', 0))
            try:
                self.staging_version = int(property.get('stagingVersion', 0))
            except TypeError:
                self.staging_version = 0
            try:
                self.production_version = int(property.get('productionVersion', 0))
            except TypeError:
                self.production_version = 0

            return property
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
        response = self.session.get(url, headers=self.headers)
        self.logger.debug(f'Collecting properties version detail {urlparse(response.url).path:<30} {response.status_code}')
        if response.status_code == 200:
            # print_json(data=response.json())
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
        response = self.session.get(url, headers=self.headers)
        self.logger.debug(f'Collecting properties version detail {urlparse(response.url).path:<40} {response.status_code}')
        if response.ok:
            propertyName = response.json()['propertyName']
            assetId = response.json()['assetId']
            gid = response.json()['groupId']
            acc_url = f'https://control.akamai.com/apps/property-manager/#/property-version/{assetId}/{version}/edit?gid={gid}'
            self.logger.debug(f'{propertyName:<46} {acc_url}')
            return response.json()
        else:
            return response.json()

    def get_property_version_detail_xml(self, property_id: int, version: int, contract_id: str, group_id: str):
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}')
        response = self.session.get(url, headers=self.headers)
        header = {'PAPI-Use-Prefixes': 'false', 'accept': 'text/xml'}
        param = {'contractId': contract_id, 'groupId': group_id}
        response = self.session.get(url, headers=header, params=param)
        return response.text

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

    def update_property_ruletree(self, property_id: int, version: int,
                                 rule_format: str,
                                 rules: dict,
                                 version_note: str,
                                 group_id: str | None = None,
                                 contract_id: str | None = None) -> tuple:
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}/rules')

        headers = {'Content-Type': 'application/vnd.akamai.papirules.latest+json'}
        headers['Accept'] = 'application/vnd.akamai.papirules.latest+json'

        if rule_format != 'latest':
            headers['Content-Type'] = f'application/vnd.akamai.papirules.{rule_format}+json'
            headers['Accept'] = f'application/vnd.akamai.papirules.{rule_format}+json'

        if 'rules' in rules.keys():
            rules = rules['rules']

        payload = {'rules': rules}
        payload['ruleFormat'] = rule_format
        payload['comments'] = version_note

        params = {'validateRules': True,
                  'validateMode': 'full'}
        if group_id and contract_id:
            params['groupId'] = group_id,
            params['contractId'] = contract_id,

        resp = self.session.put(url, json=payload, headers=headers, params=params)

        return resp

    def get_property_hostnames(self, property_id: int) -> list:
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/hostnames')
        response = self.session.get(url, headers=self.headers)
        # self.logger.debug(f'Collecting hostname for a property {urlparse(response.url).path:<30} {response.status_code} {response.url}')
        if response.ok:
            try:
                hostnames = response.json()['hostnames']['items']
                return hostnames
            except KeyError as e:
                self.logger.warning(f'{property_id=} missing {str(e)}')
        else:
            self.logger.error(f'{property_id=} error')

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

    def property_version(self, items: dict, property_name: str | None = None, order: int | None = None) -> tuple:

        latest_version = 0
        stg_version = 0
        prd_version = 0

        try:
            itemss = []
            itemss = items['versions']['items']
        except TypeError:
            self.logger.debug(f'      {property_name:<70}invalid')

        for item in itemss:
            self.property_id = item['propertyId']
            if item['stagingStatus'] == 'ACTIVE':
                stg_version = item['propertyVersion']
            if item['productionStatus'] == 'ACTIVE':
                prd_version = item['propertyVersion']

        if len(itemss) == 1:
            latest_version = itemss[0]['propertyVersion']
            stg_version = latest_version
            prd_version = stg_version
        elif len(itemss) > 1:
            dd = defaultdict(list)
            for d in itemss:
                for k, v in d.items():
                    dd[k].append(v)
            latest_version = max(dd['propertyVersion'])

        if len(itemss) == 0:
            config = f'{order:<5} {property_name}'
            self.logger.error(f'{config:<70} invalid')
        else:
            if not order:
                order = ''
                config = f'{order:<5} {itemss[0]["propertyName"]}'
            else:
                config = f'{order:<5} {itemss[0]["propertyName"]}'
            if latest_version == stg_version and stg_version == prd_version:
                self.logger.info(f'{config:<70}                     v{latest_version}')
            else:
                self.logger.info(f'{config:<70} latest_stg_prd      v{latest_version}_v{stg_version}_v{prd_version}')

        return latest_version, stg_version, prd_version

    def property_rate_limiting(self, property_id: int, version: int):
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}/rules')
        resp = self.session.get(url, headers=self.headers)
        self.headers['Accept'] = 'application/vnd.akamai.papirules.latest+json'

        ruletree_response = self.session.get(url, headers=self.headers)
        self.logger.debug(f'{urlparse(resp.url).path:<30} {resp.status_code}')
        if resp.ok:
            self.property_name = ruletree_response.json()['propertyName']
            # https://github.com/psf/requests/issues/1380
            # rate_limiting_dict = json.loads(response.headers)
            return resp.headers, ruletree_response.json()

    # RULETREE
    def property_ruletree(self, property_id: int, version: int, remove_tags: list | None = None):
        url = self.form_url(f'{self.MODULE}/properties/{property_id}/versions/{version}/rules')
        params = {
                  'validateRules': 'true',
                  'validateMode': 'full',
                 }
        resp = self.session.get(url, headers=self.headers, params=params)

        if resp.ok:
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

        # self.headers['Content-Type'] = 'application/vnd.akamai.papirules.latest+json'
        # self.headers['Accept'] = 'application/vnd.akamai.papirules.latest+json'
        # print_json(data=self.headers)

        if version > 0:
            resp = self.session.get(url, headers=self.headers, params=params)
            self.logger.debug(f'{resp.status_code} {resp.text}')
            try:
                chidren = len(resp.json()['rules']['children'])
            except KeyError:
                chidren = ''
                print_json(data=resp.json())
                self.logger.error(resp)
            self.logger.debug(f'original {chidren}')
            self.logger.debug(resp)
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
                                  note: str,
                                  email: list,
                                  reviewed_email: str | None = None):

        url = self.form_url(f'{self.MODULE}/properties/{property_id}/activations')
        payload = {'acknowledgeAllWarnings': True,
                   'activationType': 'ACTIVATE',
                   'ignoreHttpErrors': True
                   }
        if network == 'production':
            payload['complianceRecord'] = {'noncomplianceReason': 'EMERGENCY',
                                           'peerReviewedBy': reviewed_email,
                                           'unitTested': True
                                          }

        payload['network'] = network.upper()
        payload['propertyVersion'] = version
        payload['note'] = note
        payload['notifyEmails'] = email if isinstance(email, list) else [email]

        resp = self.session.post(url, json=payload, headers=self.headers)
        status = resp.status_code

        if status == 201:
            return status, resp.json()['activationLink']
        elif status == 422:
            # pending activation or deactivation
            self.logger.critical(f"{status} {resp.json()['detail']}")
            return status, resp.json()['detail']
        elif status == 429:
            # Hit rate limit
            self.logger.critical(f"{status} {resp.json()['title']}")
            return status, resp.json()['title']
        else:
            if 'errors' in resp.json().keys():
                errors = []
                for err in resp.json()['errors']:
                    errors.append(err['detail'])
                self.logger.critical(f'{property_id} {status} {errors}')
            return status, resp.json()

    def activation_status(self, property_id: int, activation_id: int):
        url = f'{self.MODULE}/properties/{property_id}/activations/{activation_id}'
        resp = self.session.get(self.form_url(url), headers=self.headers)
        if resp.ok:
            return 200, resp.json()['activations']['items']
        else:
            print_json(resp.json())
            return resp.status_code, resp.json()
        return 0, ''

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
