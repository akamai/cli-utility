from __future__ import annotations

from akamai_api.papi import PapiWrapper
from utils.exceptions import setup_logger
from utils.parser import Parser


logger = setup_logger()


class papiFunctions:
    def get_property(self, papi):
        # fancy
        logger.info(papi.account_switch_key)
        return papi.get_contracts()
