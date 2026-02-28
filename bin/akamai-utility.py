from __future__ import annotations

import platform
import subprocess
import sys
from pathlib import Path
from time import perf_counter

from akamai_api.identity_access import IdentityAccessManagement
from akamai_utils import papi as p
from command import admin
from command import bulk
from command import certificates_audit as ca
from command import cpcode as cp
from command import delivery_config as dc
from command import diff
from command import event
from command import gtm_audit as gtm
from command import log
from command import mpulse as mp
from command import report
from command import ruleformat
from command import security as sec
from command.parser import AkamaiParser as Parser
from utils import _logging as lg


if __name__ == '__main__':
    args = Parser.get_args(args=None if sys.argv[1:] else ['--help'])
    account_switch_key, section, edgerc = args.account_switch_key, args.section, args.edgerc
    start_time = perf_counter()
    logger = lg.setup_logger(args)

    if args.command is None:
        sys.exit(logger.error('no valid command found'))
    else:
        if args.command not in ['search', 'ruleformat', 'log', 'mpulse']:
            # display full account name
            if account_switch_key:
                iam = IdentityAccessManagement(account_switch_key=account_switch_key, section=section, edgerc=edgerc, logger=logger)
                account = iam.search_account_name(value=account_switch_key)[0]
            else:
                papi = p.PapiWrapper(account_switch_key=account_switch_key, section=section, edgerc=edgerc, logger=logger)
                account_id = papi.get_account_id()
                iam = IdentityAccessManagement(account_switch_key=account_id, logger=logger)
                result = iam.search_account_name(value=account_id)
                if not result:  # None or empty list ? permission issue (normal for customers)
                    print(f" [WARNING] Account name lookup failed (single-account permission). Using fallback ID: {account_id}")
                    account = account_id
                else:
                    account = result[0]
            account = iam.show_account_summary(account)
            account_folder = f'output/{account}'
            Path(account_folder).mkdir(parents=True, exist_ok=True)

    if args.command == 'mpulse':
        if args.subcommand == 'token':
            mp.generate_token(args, logger=logger)

        if args.subcommand == 'list':
            mp.list_available_account(args, logger=logger)

        if args.subcommand == 'url':
            mp.url(args, logger=logger)

        if args.subcommand == 'pageload':
            mp.pageload_overtime(args, logger=logger)

    if args.command == 'delivery':
        if args.subcommand == 'behavior':
            dc.get_property_all_behaviors(args, logger=logger)
        elif args.subcommand == 'custom-behavior':
            dc.get_custom_behavior(args, logger=logger)
        elif args.subcommand == 'metadata':
            dc.get_property_advanced_behavior(args, account_folder, logger)
        elif args.subcommand == 'ruletree':
            Path(f'{account_folder}/ruletree').mkdir(parents=True, exist_ok=True)
            Path(f'{account_folder}/ruletree/hierachy').mkdir(parents=True, exist_ok=True)
            Path(f'{account_folder}/ruletree/limit').mkdir(parents=True, exist_ok=True)
            dc.get_property_ruletree(args, account_folder, logger=logger)
        elif args.subcommand == 'hostname-cert':
            dc.hostnames_certificate(args, account_folder, logger=logger)
        elif args.subcommand == 'netstorage':
            dc.netstorage(args, account_folder, logger=logger)
        # elif args.subcommand == 'shp':
        #    dc.jsonpath_group(args, account_folder, logger=logger)
        elif args.subcommand == 'origin-cert':
            dc.origin_certificate(args, account_folder, logger=logger)
        elif args.subcommand == 'jsonpath':
            dc.jsonpath(args, account_folder, logger=logger)
        elif args.subcommand == 'ruleformat':
            Path(f'{account_folder}/ruleformat').mkdir(parents=True, exist_ok=True)
            dc.upgrade_ruleformat(args, account_folder, logger=logger)
        else:
            dc.main(args, account_folder, logger)

    if args.command == 'security':
        account_folder = f'{account_folder}/security'
        Path(f'{account_folder}').mkdir(parents=True, exist_ok=True)
        if args.subcommand == 'hostname':
            sec.audit_hostname(args, account_folder, logger)
        else:
            sec.list_config(args, account_folder, logger)

    if args.command == 'diff':
        proceed = False
        if platform.system() != 'Darwin':
            sys.exit(logger.info('diff command only support on non-Window OS'))

        try:
            cmd_text = 'pip list | grep ydiff'
            grep = subprocess.run(cmd_text, shell=True, stdout=subprocess.PIPE)
            if grep.stdout != b'':
                proceed = True
            else:
                cmd_text = 'pip install ydiff'
                ydiff = subprocess.run(cmd_text, shell=True, stdout=subprocess.PIPE)
                if ydiff.stdout != b'':
                    proceed = True
                else:
                    logger.error('subprocess error for pip install ydiff')
        except subprocess.CalledProcessError as e:
            logger.error('subprocess error for pip install ydiff')
            logger.error(e.stdout)
            logger.error(e.stderr)

        if proceed:
            if args.subcommand == 'behavior':
                diff.compare_delivery_behaviors(args, logger=logger)
            else:
                diff.compare_config(args, logger=logger)

    if args.command == 'bulk':
        Path(f'{account_folder}/bulk').mkdir(parents=True, exist_ok=True)
        if args.subcommand == 'search':
            bulk.bulk_search(args, account_folder, logger=logger)
        elif args.subcommand == 'combine':
            bulk.combine(args, account_folder, logger=logger)
        elif args.subcommand == 'create':
            bulk.bulk_create(args, account_folder, logger=logger)
        elif args.subcommand == 'update':
            bulk.bulk_update(args, account_folder, logger=logger)
        elif args.subcommand == 'activate':
            bulk.bulk_activate(args, account_folder, logger=logger)
        elif args.subcommand == 'add':
            bulk.bulk_add_behavior_default(args, account_folder, logger=logger)

    if args.command == 'certificate':
        ca.audit(args, account_folder, logger)

    if args.command == 'cpcode':
        if args.subcommand == 'reporting':
            if args.ops == 'create':
                if not args.input:
                    sys.exit('input excel file is required')
                cp.create_reporting_group(args, logger=logger)
            elif args.ops == 'update':
                cp.update_reporting_group(args, logger=logger)
            elif args.ops == 'delete':
                cp.delete_reporting_group(args, logger=logger)
            else:
                cp.list_reporting_group(args, account_folder, logger=logger)
        else:
            cp.list_cpcode(args, account_folder, logger=logger)

    if args.command == 'event':
        Path(f'{account_folder}').mkdir(parents=True, exist_ok=True)
        if args.subcommand == 'create':
            event.create_event(args, logger=logger)
        elif args.subcommand == 'detail':
            event.get_event(args, account_folder, logger=logger)
        elif args.subcommand == 'remove':
            event.remove_event(args, logger=logger)
        else:
            event.list_events(args, logger=logger)

    if args.command == 'gtm':
        if args.subcommand == 'remove':
            gtm.remove_gtm_property(args, logger)
        else:
            gtm.audit(args, account_folder, logger)

    if args.command == 'report':
        account_folder = f'output/{account}/report'
        Path(account_folder).mkdir(parents=True, exist_ok=True)
        if args.subcommand == 'list':
            report.all_reports(args, account_folder, logger)
        elif args.subcommand == 'offload-file-extension':
            report.offload_by_url(args, logger)
        elif args.subcommand == 'offload-hostname':
            report.offload_by_hostname(args, account_folder, logger)
        elif args.subcommand == 'response-class':
            report.traffic_by_response_class(args, account_folder, logger)
            # report.traffic_by_response_class_async(args)

    if args.command == 'ruleformat':
        ruleformat.get_ruleformat_schema(args, logger)

    if args.command == 'log':
        log.main(args, logger)

    if args.command == 'search':
        admin.lookup_account(args, logger)

    if args.command == 'self':
        admin.get_api_client(args, logger)

    if args.command not in ['search', 'ruleformat', 'self']:
        end_time = lg.log_cli_timing(start_time)
        logger.info(end_time)
