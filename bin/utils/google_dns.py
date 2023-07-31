from __future__ import annotations

import logging

import requests


def dnslookup(hostname: str | None = None, logger: logging.Logger = None):
    if isinstance(hostname, (float, int)):
        return 'Wildcard'  # or any desired value for float or int types
    else:
        acme_request = f'https://dns.google/resolve?name={hostname}&do=0'

        result = requests.get(acme_request)
        output = 'None'
        if result.status_code == 200:
            if result.json()['Status'] == 0:
                if 'Answer' in result.json():
                    for cname in result.json()['Answer']:
                        output = result.json()['Answer'][0]['data']
                        output = cname['data']
                        if 'edgekey.net' in cname['data']:
                            break
                        if 'edgesuite.net' in cname['data']:
                            logger.debug(f'{hostname:<40} cname to {output}')
                            break
            elif result.json()['Status'] == 3:
                output = 'None'
            elif result.json()['Status'] == 2:
                output = 'DNS SERV FAIL'
            else:
                output = 'N/A'
        return output


if __name__ == '__main__':
    pass
