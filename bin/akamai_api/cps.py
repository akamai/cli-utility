from __future__ import annotations

from urllib.parse import urlparse

import pandas as pd
from akamai_api.edge_auth import AkamaiSession
from utils import _logging as lg
from utils import google_dns as gg


logger = lg.setup_logger()


class CpsWrapper(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None, section: str | None = None, cookies: str | None = None):
        super().__init__(account_switch_key=account_switch_key, section=section, cookies=cookies)
        self.MODULE = f'{self.base_url}/cps/v2'
        self.headers = {'Accept': 'application/vnd.akamai.cps.enrollments.v11+json'}
        self.account_switch_key = account_switch_key if account_switch_key else None

    def list_enrollments(self, contract_id: str, enrollment_ids: list | None = None) -> list:
        logger.debug(f'{contract_id=} {enrollment_ids=}')

        params = self.params
        params['contractId'] = contract_id
        response = self.session.get(f'{self.MODULE}/enrollments', params=params, headers=self.headers)
        enrollments = []
        if response.status_code == 200:
            logger.debug(f'Enrollments for contract {contract_id:<15} {urlparse(response.url).path:>20} {response.status_code}')
            enrollments = response.json()['enrollments']

            df = pd.DataFrame(enrollments)
            pd.set_option('display.max_rows', 300)
            pd.set_option('max_colwidth', 50)
            df['productionSlots'] = df['productionSlots'].apply(lambda x: x[0] if len(x) == 1 else 0)
            df['common_name'] = df['csr'].apply(lambda x: x['cn'])
            df['hostname_count'] = df['csr'].apply(lambda x: len(x['sans']))
            df['sni'] = df['networkConfiguration'].apply(lambda x: x['sniOnly'])

            empty_df = df[df['hostname_count'] == 0].copy()
            good_df = df[df['hostname_count'] > 0].copy()

            columns = ['id', 'productionSlots', 'ra', 'common_name', 'hostname_count', 'sni']
            good_df = good_df.sort_values(by='common_name')
            good_df = good_df.reset_index(drop=True)

            df = df.sort_values(by=['common_name'])
            df = df.reset_index(drop=True)
            logger.debug(f'\n{df[columns]}')

            logger.warning(f'out of {df.shape[0]}, {empty_df.shape[0]} certificates do have hostname assigned to')
            return enrollments, df, good_df[columns]
        else:
            logger.error(f'Enrollments for contract {contract_id:<15} {urlparse(response.url).path:>20} {response.status_code}')
            return [], pd.DataFrame(), pd.DataFrame()

    def collect_enrollments(self, contract_id: str, enrollments: list, enrollment_ids: list | None = None) -> list:
        enrollment_filered = [cert for cert in enrollments if cert.get('id', {}) in enrollment_ids]

        enrollment_subset = []
        if enrollment_filered:
            for certificates in enrollment_filered:
                temp_list = self.enrollment_detail(certificates['id'], certificates)
                df = pd.DataFrame(temp_list)
                enrollment_subset.extend(temp_list)
                logger.debug(f'enrollment id: {certificates["id"]} {len(temp_list)}')
        else:
            logger.debug('Collect certificate detail ie. cName and expiration date.  Please be patient.')
            for i, certificates in enumerate(enrollments):
                try:
                    temp_list = self.enrollment_detail(contract_id, certificates)
                except:
                    logger.debug(f'{contract_id=:<20} {certificates["id"]=}')
                finally:
                    try:
                        slot = certificates['productionSlots'][0]
                    except:
                        slot = 0
                    try:
                        common_name = certificates['csr']['cn']
                    except:
                        common_name = ''
                    logger.debug(f'{i:<5} {slot:<10} {common_name:<50} {len(temp_list):>4} hostnames')
                    if len(temp_list) > 0:
                        enrollment_subset.extend(temp_list)
        logger.info(f'{contract_id=} Size={len(enrollment_subset)}')
        df = pd.DataFrame(enrollment_subset)
        logger.debug(f'\n{df}')
        return enrollment_subset

    def enrollment_detail(self, contract_id: str, certificates: dict):
        enrollment_id = certificates['id']
        try:
            expire_date, _ = self.certificate_deployment(enrollment_id=enrollment_id)
        except:
            logger.debug(f'{contract_id=:<20} {enrollment_id=:<20} no expire_date')

        # from rich import print_json
        # print_json(data=certificates)
        common_name = certificates['csr']['cn']
        ra = certificates['ra']
        try:
            slot = certificates['productionSlots'][0]
        except:
            slot = 'slot not assigned'

        # if SAN, list all hostnames
        enrollment_subset = []
        if certificates['csr']['sans']:
            hostnames = certificates['csr']['sans']
            '''
            enrollment_subset.append({'contractId': contract_id,
                                          'enrollmentId': enrollment_id,
                                          'slot': slot,
                                          'SNI': True,
                                          'hostname': len(hostnames),
                                          'common_name': common_name,
                                          'CNAME': cname,
                                          'ra': ra,
                                          'expire_date': expire_date,
                                          'autoRenewalStartTime': certificates['autoRenewalStartTime'],
                                          'adminContact' : certificates['adminContact']['email'],
                                          'techContact' : certificates['techContact']['email']
                                          })
            '''
            for hostname in certificates['csr']['sans']:
                cname = gg.dnslookup(hostname=hostname)
                if 'edgesuite.net' in cname:
                    logger.info(f'{contract_id=:<20} {enrollment_id=:<20} {hostname:<40} cname to {cname}')
                enrollment_subset.append({'contractId': contract_id,
                                          'enrollmentId': enrollment_id,
                                          'slot': slot,
                                          'SNI': True,
                                          'hostname': hostname,
                                          'common_name': common_name,
                                          'CNAME': cname,
                                          'ra': ra,
                                          'expire_date': expire_date,
                                          'autoRenewalStartTime': certificates['autoRenewalStartTime'],
                                          'adminContact': certificates['adminContact']['email'],
                                          'techContact': certificates['techContact']['email']
                                          })

        else:
            cname = gg.dnslookup(hostname=common_name)
            # if 'edgesuite.net' in cname:
            # logger.info(f'{contract_id=:<20} {enrollment_id=:<20} {hostname:<40} cname to {cname}')
            enrollment_subset.append({'contractId': contract_id,
                                      'enrollmentId': enrollment_id,
                                      'slot': slot,
                                      'SNI': False,
                                      'hostname': common_name,
                                      'common_name': common_name,
                                      'CNAME': cname,
                                      'ra': ra,
                                      'expire_date': expire_date,
                                      'autoRenewalStartTime': certificates['autoRenewalStartTime'],
                                      'adminContact': certificates['adminContact']['email'],
                                      'techContact': certificates['techContact']['email']
                                      })

        return enrollment_subset

    def certificate_deployment(self, enrollment_id: int) -> str:
        self.headers = {'Accept': 'application/vnd.akamai.cps.deployments.v7+json'}
        url = f'{self.MODULE}/enrollments/{enrollment_id}/deployments'
        response = self.session.get(url, params=self.params, headers=self.headers)
        expire_date = None
        if response.status_code == 200:
            try:
                expire_date = response.json()['production']['primaryCertificate']['expiry']
            except KeyError:
                logger.error(f'{enrollment_id=}')
        else:
            logger.error(f'Deployment for {enrollment_id} {urlparse(response.url).path:>20} {response.status_code}')
        return expire_date, response.json()['production']['primaryCertificate']


if __name__ == '__main__':
    pass
