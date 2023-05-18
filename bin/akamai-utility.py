from __future__ import annotations

from command import delivery_config
from command import diff
from command import report
from command import ruleformat
from utils._logging import setup_logger
from utils.parser import AkamaiParser as Parser

logger = setup_logger()

if __name__ == '__main__':
    args = Parser.get_args()

    if args.command == 'diff':
        diff.main(args)

    if args.command == 'ruleformat':
        ruleformat.get_ruleformat_schema(args)

    if args.command == 'report':
        report.offload(args)

    if args.command == 'delivery-config':
        delivery_config.main(args)
