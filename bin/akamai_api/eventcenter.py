# Techdocs reference
# https://techdocs.akamai.com/event-ctr/reference/api
from __future__ import annotations

import logging

from akamai_api.edge_auth import AkamaiSession
from utils import _logging as lg


class EventCenter(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 contract_id: str | None = None,
                 group_id: int | None = None,
                 logger: logging.Logger = None):
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
        self._base_url = f'{self.base_url}/events/v3/'
        self.headers = {'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'PAPI-Use-Prefixes': 'false',
                        }
        self.account_switch_key = account_switch_key
        self.contract_id = contract_id
        self.group_id = group_id
        self.logger = logger

    # EVENTS
    def list_events(self):
        resp = self.session.get(f'{self._base_url}/events?pageSize=1000&sort=start,desc&status=ALL', params=self.params, headers=self.headers)
        return resp

    def create_event(self, payload: dict):
        resp = self.session.post(f'{self._base_url}/events', json=payload, params=self.params, headers=self.headers)
        return resp

    def remove_event(self, id: int):
        headers = {'accept': 'application/problem+json'}
        resp = self.session.delete(f'{self._base_url}/events/{id}', headers=headers)
        return resp

    def get_event(self, id: int):
        resp = self.session.get(f'{self._base_url}/events/{id}', params=self.params, headers=self.headers)
        return resp

    def update_event(self, id: int):
        resp = self.session.put(f'{self._base_url}/events/{id}', params=self.params, headers=self.headers)
        return resp

    # TAGS
    def list_tags(self):
        resp = self.session.get(f'{self._base_url}/tags', params=self.params, headers=self.headers)
        return resp


if __name__ == '__main__':
    pass
