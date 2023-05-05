from __future__ import annotations

from akamai_api.papi import PapiWrapper
from utils.exceptions import setup_logger
from utils.papi import papiFunctions
from utils.parser import Parser

logger = setup_logger()

if __name__ == '__main__':
    args = Parser.get_args()
    papi = PapiWrapper(account_key=args.account_switch_key,
                       contract_id=args.contract_id,
                       group_id=args.group_id)
    util = papiFunctions()
    property = util.get_property(papi)
    logger.info(property)
    logger.info(papi.account_switch_key)

    papi.account_switch_key = 'julie'
    logger.info(papi.account_switch_key)

    property = util.get_property(papi)
