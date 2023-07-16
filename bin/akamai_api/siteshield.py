# Techdocs reference
# https://techdocs.akamai.com/site-shield/reference/api-summary
from __future__ import annotations

from akamai_api.edge_auth import AkamaiSession
from utils import _logging as lg


logger = lg.setup_logger()


class SiteShield(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None, section: str | None = None, cookies: str | None = None):
        super().__init__(account_switch_key=account_switch_key, section=section, cookies=cookies)
        self.MODULE = f'{self.base_url}/siteshield/v1'
        self.headers = {'Accept': 'application/json'}
        self.account_switch_key = account_switch_key if account_switch_key else None

    def list_maps(self) -> list:
        resp = self.session.get(f'{self.MODULE}/maps', params=self.params, headers=self.headers)
        if resp.status_code == 200:
            siteShieldMaps = resp.json()['siteShieldMaps']
            logger.debug(len(siteShieldMaps))
            return resp.json()['siteShieldMaps']

    def get_map(self, map_id: int) -> list:
        resp = self.session.get(f'{self.MODULE}/maps/{map_id}', params=self.params, headers=self.headers)
        if resp.status_code == 200:
            return resp.json()['currentCidrs']
