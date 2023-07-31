# Techdocs reference
# https://techdocs.akamai.com/bot-manager/reference/api-summary
from __future__ import annotations

import logging
import sys

from akamai_api.edge_auth import AkamaiSession
from boltons.iterutils import remap
from rich import print_json
from utils import _logging as lg
from utils import files


class BotManager(AkamaiSession):
    def __init__(self, account_switch_key: str | None = None, section: str | None = None,
                logger: logging.Logger = None):
        super().__init__(account_switch_key=account_switch_key, section=section)
        self.MODULE = f'{self.base_url}/appsec/v1'
        self.headers = {'Accept': 'application/json',
                        'Content-Type': 'application/json'}
        self.contract_id = self.contract_id
        self.group_id = self.group_id
        self.config_id = None
        self.account_switch_key = account_switch_key
        self.logger = logger

    def get_all_akamai_bot_catagories(self):
        url = self.form_url(f'{self.MODULE}/akamai-bot-categories')
        response = self.session.get(url, headers=self.headers)
        return response.status_code, response.json()

    def get_akamai_bot_catagory(self, id: str):
        url = self.form_url(f'{self.MODULE}/akamai-bot-categories/{id}')
        response = self.session.get(url, headers=self.headers)
        return response.status_code, response.json()

    def get_all_custom_bot_catagories(self, config_id: str, version: int):
        url = self.form_url(f'{self.MODULE}/configs/{config_id}/versions/{version}/custom-bot-categories')
        response = self.session.get(url, headers=self.headers)
        return response.status_code, response.json()

    def get_custom_bot_catagory(self, config_id: str, version: int, category_id: str):
        url = self.form_url(f'{self.MODULE}/configs/{config_id}/versions/{version}/custom-bot-categories/{category_id}')
        response = self.session.get(url, headers=self.headers)
        return response.status_code, response.json()

    def get_custom_bot_catagory_action(self, config_id: str, version: int, policy_id: str, category_id: str):
        url = self.form_url(f'{self.MODULE}/configs/{config_id}/versions/{version}/security-policies/{policy_id}/custom-bot-category-actions/{category_id}')
        response = self.session.get(url, headers=self.headers)
        return response.status_code, response.json()

    def get_custom_bot_catagory_sequence(self, config_id: str, version: int):
        url = self.form_url(f'{self.MODULE}/configs/{config_id}/versions/{version}/custom-bot-category-sequence')
        response = self.session.get(url, headers=self.headers)
        return response.status_code, response.json()

    def get_custom_defined_bot(self, config_id: str, version: int, bot_id: str):
        url = self.form_url(f'{self.MODULE}/configs/{config_id}/versions/{version}/custom-defined-bots/{bot_id}')
        response = self.session.get(url, headers=self.headers)
        return response.status_code, response.json()


if __name__ == '__main__':
    pass
