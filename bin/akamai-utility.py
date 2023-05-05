from __future__ import annotations

from akamai_api.papi import PapiWrapper
from command import offload
from utils.exceptions import setup_logger
from utils.papi import papiFunctions
from utils.parser import Parser

logger = setup_logger()

if __name__ == '__main__':
    args = Parser.get_args()

    if args.command == 'offload':
        offload.main(args.account_switch_key)
