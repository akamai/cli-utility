from __future__ import annotations

import ipaddress
import logging

import pandas as pd
from akamai_api.siteshield import SiteShield
from utils import _logging as lg


class SiteShieldWrapper(SiteShield):
    def __init__(self, account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 logger: logging.Logger = None):
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
        self.account_switch_key = account_switch_key
        self.logger = logger

    def list_maps(self):
        ss_map = super().list_maps()
        df = pd.DataFrame(ss_map)
        self.logger.debug(df.columns)
        columns = ['mapAlias', 'id', 'mcmMapRuleId', 'ruleName', 'sureRouteName', 'type', 'service', 'shared']
        df = df.sort_values(by='mapAlias').reset_index(drop=True)
        self.logger.debug(f'\n{df[columns]}')
        return df[columns]

    def get_map(self, map_alias: str, map_id: int, rule: str) -> dict:
        all_ss_ips = []
        ss_dict = {}
        ips = super().get_map(map_id)
        for i, ip in enumerate(ips):
            x = [str(ip) for ip in ipaddress.ip_network(ip, strict=False)]
            all_ss_ips.extend(x)
            if i == len(ips) - 1:
                ss_dict[rule] = {'map_id': int(map_id),
                                 'map_alias': map_alias,
                                 'ips_size': len(all_ss_ips),
                                 'cidr': [str(item) for item in ips],
                                 'all_ips': all_ss_ips}
        return ss_dict


if __name__ == '__main__':
    pass
