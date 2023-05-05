from __future__ import annotations

import argparse
from argparse import HelpFormatter
from operator import attrgetter

from rich_argparse import RichHelpFormatter


class SortingHelpFormatter(HelpFormatter):
    # https://stackoverflow.com/questions/12268602/sort-argparse-help-alphabetically
    def add_arguments(self, actions):
        actions = sorted(actions, key=attrgetter('help'))
        super().add_arguments(actions)


class Parser:
    def __init__(self):
        pass

    @classmethod
    def get_args(cls):
        parser = argparse.ArgumentParser(prog='Akamai API',
                                         description='Available arguments',
                                         epilog='Options are sorted alphabetically. List options with both short and long arguments first',
                                         formatter_class=RichHelpFormatter)

        # https://stackoverflow.com/questions/12268602/sort-argparse-help-alphabetically
        # parser = argparse.ArgumentParser(description='Available arguments', formatter_class=SortingHelpFormatter)

        # https://stackoverflow.com/questions/52605094/python-argparse-increase-space-between-parameter-and-description
        # formatter = lambda prog: argparse.HelpFormatter(prog, max_help_position=88)

        # arguments with both short and long options
        parser.add_argument('-a', '--account-key', '--account-switchkey', '--accountswitchkey',
                            metavar='', type=str, default=None,
                            dest='account_switch_key',
                            help='account switch key (Internal Only)')

        parser.add_argument('-c', '--contract-id',
                            metavar='', type=str, default=None,
                            dest='contract_id',
                            help='contract id without ctr_ prefix')

        parser.add_argument('-e', '--edgehostname-id',
                            metavar='', type=int, default=None,
                            dest='edgehostname_id',
                            help='edge hostname id without ehn_ prefix')

        parser.add_argument('-g', '--group-id',
                            metavar='', type=int, default=None,
                            dest='group_id',
                            help='group id without grp_ prefix')

        parser.add_argument('-s', '--search',
                            metavar='', type=str, default=None,
                            dest='search_value', action='append',
                            help='search value. This can be property name, contract id, edgehostname, and etc.')

        parser.add_argument('-i', '--id', '--property-id', '--config-id', '--search-id',
                            metavar='', type=str, default=None,
                            dest='search_id',
                            help='search any id such as property ID or security ID')

        parser.add_argument('-v', '--version', '--property-version', '--security-version',
                            metavar='', type=int, default=None,
                            dest='version',
                            help='version for either property or security config')

        # choices
        parser.add_argument('--network', metavar='', default='STAGING',
                            dest='network', choices=['STAGING', 'PRODUCTION'],
                            help='choice is either STAGING or PRODUCTION')

        # arguments without short options
        parser.add_argument('--account-name', metavar='', type=str, default=None,
                            dest='account_name',
                            help='account name')
        parser.add_argument('--comment', '--note', metavar='', type=str, default=None,
                            dest='comment',
                            help='comment/note for property or security config, or activation message')

        # file types
        parser.add_argument('--file', metavar='', type=str, default=None,
                            dest='input_file',
                            help='General any type of input file')
        parser.add_argument('--csv', metavar='', type=str, default=None,
                            dest='csv_file',
                            help='CSV filename to be placed under output directory')
        parser.add_argument('--json', metavar='', type=str, default=None,
                            dest='json_file',
                            help='JSON filename to be placed under output directory')
        parser.add_argument('--log', metavar='', type=str, default=None,
                            dest='log_file',
                            help='LOG filename to be placed under logs directory')
        parser.add_argument('--txt', metavar='', type=str, default=None,
                            dest='text_file',
                            help='TXT filename to be placed under logs directory')
        parser.add_argument('--xlsx', metavar='', type=str, default=None,
                            dest='xlsx_file',
                            help='XLSX filename to be placed under output directory')

        # https://stackoverflow.com/questions/11999416/python-argparse-metavar-and-action-store-true-together

        parser.add_argument('--dryrun',
                            action='store_true',
                            dest='dryrun',
                            default=False,
                            help='True will not deactivate or delete configs.')
        parser.add_argument('--show',
                            action='store_true',
                            dest='show',
                            default=False,
                            help='automatically open application')
        parser.add_argument('--verbose',
                            action='store_true',
                            dest='verbose',
                            default=False,
                            help='increase output/logging verbosity')

        subparsers = parser.add_subparsers(dest='command')
        parser_offload = subparsers.add_parser('offload')
        parser_offload.add_argument('-p',
                            metavar='', type=str, default=None,
                            dest='product',
                            help='product list')

        return parser.parse_args()
