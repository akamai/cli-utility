# Techdocs reference
# https://techdocs.akamai.com/event-ctr/reference/api
from __future__ import annotations

from akamai_api.eventcenter import EventCenter


class EventCenterWrapper(EventCenter):
    def __init__(self, account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None):
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
        self.account_switch_key = account_switch_key

    # EVENTS
    def list_events(self):
        resp = super().list_events()
        return resp

    def create_event(self, payload: dict):
        resp = super().create_event(payload)
        return resp

    def remove_event(self, id: int):
        resp = super().remove_event(id)
        return resp

    def get_event(self, id: int):
        resp = super().get_event(id)
        return resp

    def update_event(self, id: int):
        resp = super().update_event(id)
        return resp

    # TAGS
    def list_tags(self):
        resp = super().list_tags()
        return resp


if __name__ == '__main__':
    pass
