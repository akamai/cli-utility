from __future__ import annotations

from pathlib import Path
from time import perf_counter

from command import admin
from command import certificates_audit as ca
from command import delivery_config as dc
from command import diff
from command import gtm_audit as gtm
from command import log
from command import report
from command import ruleformat
from command import security as sec
from utils import _logging as lg
from utils.parser import AkamaiParser as Parser


logger = lg.setup_logger()


if __name__ == '__main__':
    args = Parser.get_args()
    start_time = perf_counter()

    if args.command == 'delivery-config':
        Path('output').mkdir(parents=True, exist_ok=True)
        if args.subcommand == 'behavior':
            dc.get_property_all_behaviors(args)
        elif args.subcommand == 'custom-behavior':
            dc.get_custom_behavior(args)
        elif args.subcommand == 'metadata':
            dc.get_property_advanced_behavior(args)
        elif args.subcommand == 'activate':
            dc.activate_from_excel(args)
        elif args.subcommand == 'ruletree':
            dc.get_property_ruletree(args)
        elif args.subcommand == 'hostname-cert':
            dc.hostnames_certificate(args)
        elif args.subcommand == 'origin-cert':
            dc.origin_certificate(args)
        else:
            dc.main(args)

    if args.command == 'diff':
        if args.subcommand == 'behavior':
            diff.compare_delivery_behaviors(args)
        else:
            diff.compare_config(args)

    if args.command == 'certificate':
        ca.audit(args)

    if args.command == 'gtm':
        gtm.audit(args)

    if args.command == 'log':
        log.main(args)

    if args.command == 'report':
        if args.url_offload is True:
            report.url_offload(args)
        else:
            report.offload(args)

    if args.command == 'ruleformat':
        ruleformat.get_ruleformat_schema(args)

    if args.command == 'search':
        admin.lookup_account(args)

    if args.command == 'security':
        sec.list_configs(args)

    end_time = lg.log_cli_timing(start_time)
    logger.info(end_time)
