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


if __name__ == '__main__':
    args = Parser.get_args()
    logger = lg.setup_logger(args)
    start_time = perf_counter()

    if args.command == 'delivery-config':
        Path('output').mkdir(parents=True, exist_ok=True)
        if args.subcommand == 'behavior':
            dc.get_property_all_behaviors(args, logger=logger)
        elif args.subcommand == 'custom-behavior':
            dc.get_custom_behavior(args, logger=logger)
        elif args.subcommand == 'metadata':
            dc.get_property_advanced_behavior(args, logger)
        elif args.subcommand == 'activate':
            dc.activate_from_excel(args, logger=logger)
        elif args.subcommand == 'ruletree':
            dc.get_property_ruletree(args, logger=logger)
        elif args.subcommand == 'hostname-cert':
            dc.hostnames_certificate(args, logger=logger)
        elif args.subcommand == 'netstorage':
            dc.netstorage(args, logger=logger)
        elif args.subcommand == 'origin-cert':
            dc.origin_certificate(args, logger=logger)
        else:
            dc.main(args, logger=logger)

    if args.command == 'diff':
        if args.subcommand == 'behavior':
            diff.compare_delivery_behaviors(args, logger)
        else:
            diff.compare_config(args, logger)

    if args.command == 'certificate':
        ca.audit(args, logger)

    if args.command == 'gtm':
        gtm.audit(args, logger)

    if args.command == 'log':
        log.main(args, logger)

    if args.command == 'report':
        if args.subcommand == 'list':
            report.all_reports(args, logger)
        elif args.subcommand == 'offload-url':
            report.offload_by_url(args, logger)
        elif args.subcommand == 'offload-hostname':
            report.offload_by_hostname(args, logger)
        elif args.subcommand == 'response-class':
            report.traffic_by_response_class(args, logger)
            # report.traffic_by_response_class_async(args)

    if args.command == 'ruleformat':
        ruleformat.get_ruleformat_schema(args, logger)

    if args.command == 'search':
        admin.lookup_account(args, logger)

    if args.command == 'security':
        if args.subcommand == 'hostname':
            sec.audit_hostname(args, logger)
        else:
            sec.list_config(args, logger)

    end_time = lg.log_cli_timing(start_time)
    logger.info(end_time)
