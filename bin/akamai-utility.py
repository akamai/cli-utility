from __future__ import annotations

import sys
from pathlib import Path
from time import perf_counter

from akamai_api.identity_access import IdentityAccessManagement
from akamai_utils import papi as p
from command import admin
from command import certificates_audit as ca
from command import delivery_config as dc
from command import diff
from command import gtm_audit as gtm
from command import log
from command import report
from command import ruleformat
from command import security as sec
from command.parser import AkamaiParser as Parser
from utils import _logging as lg

if __name__ == '__main__':
    args = Parser.get_args(args=None if sys.argv[1:] else ['--help'])
    start_time = perf_counter()
    logger = lg.setup_logger(args)

    if args.command is None:
        sys.exit(logger.error('no valid command found'))
    else:
        if args.command not in ['search', 'ruleformat', 'log']:
            # display full account name
            if args.account_switch_key:
                iam = IdentityAccessManagement(account_switch_key=args.account_switch_key, section=args.section, edgerc=args.edgerc, logger=logger)
                account = iam.search_account_name(value=args.account_switch_key)[0]
            else:
                papi = p.PapiWrapper(account_switch_key=args.account_switch_key, section=args.section, edgerc=args.edgerc, logger=logger)
                account_id = papi.get_account_id()
                iam = IdentityAccessManagement(account_switch_key=account_id, logger=logger)
                account = iam.search_account_name(value=account_id)[0]
            account = iam.show_account_summary(account)
            account_folder = f'output/{account}'
            Path(account_folder).mkdir(parents=True, exist_ok=True)

    if args.command == 'delivery':
        if args.subcommand == 'behavior':
            dc.get_property_all_behaviors(args, logger=logger)
        elif args.subcommand == 'custom-behavior':
            dc.get_custom_behavior(args, logger=logger)
        elif args.subcommand == 'metadata':
            dc.get_property_advanced_behavior(args, account_folder, logger)
        elif args.subcommand == 'activate':
            dc.activate_from_excel(args, logger=logger)
        elif args.subcommand == 'ruletree':
            Path(f'{account_folder}/ruletree').mkdir(parents=True, exist_ok=True)
            dc.get_property_ruletree(args, account_folder, logger=logger)
        elif args.subcommand == 'hostname-cert':
            dc.hostnames_certificate(args, account_folder, logger=logger)
        elif args.subcommand == 'netstorage':
            dc.netstorage(args, account_folder, logger=logger)
        elif args.subcommand == 'origin-cert':
            dc.origin_certificate(args, account_folder, logger=logger)
        else:
            dc.main(args, account_folder, logger)

    if args.command == 'security':
        if args.subcommand == 'hostname':
            sec.audit_hostname(args, account_folder, logger)
        else:
            sec.list_config(args, account_folder, logger)

    if args.command == 'diff':
        if args.subcommand == 'behavior':
            diff.compare_delivery_behaviors(args, logger=logger)
        else:
            diff.compare_config(args, logger=logger)

    if args.command == 'certificate':
        ca.audit(args, account_folder, logger)

    if args.command == 'gtm':
        if args.subcommand == 'remove':
            gtm.remove_gtm_property(args, logger)
        else:
            gtm.audit(args, account_folder, logger)

    if args.command == 'report':
        account_folder = f'output/{account}/report'
        Path(account_folder).mkdir(parents=True, exist_ok=True)
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

    if args.command == 'log':
        log.main(args, logger)

    if args.command == 'search':
        admin.lookup_account(args, logger)

    if args.command not in ['search', 'ruleformat', 'log']:
        end_time = lg.log_cli_timing(start_time)
        logger.info(end_time)
