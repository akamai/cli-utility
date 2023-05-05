from __future__ import annotations

from akamai_api.papi import PapiWrapper
from utils.exceptions import setup_logger
from utils.papi import papiFunctions


logger = setup_logger()


def main(account_switch_key):
    papi = PapiWrapper(account_key=account_switch_key)
    util = papiFunctions()
    property = util.get_property(papi)
    logger.info(property)
    logger.info(papi.account_switch_key)

    papi.account_switch_key = 'julie'
    logger.info(papi.account_switch_key)

    property = util.get_property(papi)
