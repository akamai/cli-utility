from __future__ import annotations

from time import perf_counter

from command import admin
from command import delivery_config as dc
from command import diff
from command import log
from command import report
from command import ruleformat
from utils import _logging as lg
from utils.parser import AkamaiParser as Parser


logger = lg.setup_logger()


if __name__ == '__main__':
    args = Parser.get_args()
    start_time = perf_counter()

    if args.command == 'admin':
        admin.lookup_account(args)

    if args.command == 'log':
        log.main(args.input, args.output, args.search)

    if args.command == 'diff':
        diff.main(args)

    if args.command == 'ruleformat':
        ruleformat.get_ruleformat_schema(args)

    if args.command == 'report':

        if args.url_offload is True:
            report.url_offload(args)
        else:
            report.offload(args)

    if args.command == 'delivery-config':

        if args.activate is True:
            dc.rollback(args)
        elif args.load:
            dc.activation_status(args)
        elif args.ruletree:
            dc.get_property_ruletree(args)
        else:
            dc.main(args)

    end_time = lg.log_cli_timing(start_time)
    logger.info(end_time)
