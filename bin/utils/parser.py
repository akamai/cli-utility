from __future__ import annotations

import argparse
from argparse import HelpFormatter
from operator import attrgetter

import rich_argparse as rap
from utils._logging import setup_logger

logger = setup_logger()


class SortingHelpFormatter(HelpFormatter):
    # https://stackoverflow.com/questions/12268602/sort-argparse-help-alphabetically
    def add_arguments(self, actions):
        actions = sorted(actions, key=attrgetter('help'))
        super().add_arguments(actions)


class AkamaiParser(rap.RichHelpFormatter, argparse.HelpFormatter):
    def __init__(self, prog):
        super().__init__(prog,
                         indent_increment=2,
                         max_help_position=120)

    @classmethod
    def create_sub_command(cls, subparsers, name, help, required_arguments=None, optional_arguments=None):
        action = subparsers.add_parser(prog=help, usage='%(prog)s',
                                       name=name, help=help, add_help=True,
                                       formatter_class=rap.ArgumentDefaultsRichHelpFormatter)

        if required_arguments:
            required = action.add_argument_group('required arguments')
            for arg in required_arguments:
                name = arg['name']
                del arg['name']
                try:
                    action_value = arg['action']
                    del arg['action']
                    required.add_argument(f'--{name}', required=True, action=action_value, **arg)
                except:
                    required.add_argument(f'--{name}', metavar='', required=True, **arg)

        if optional_arguments:
            optional = action.add_argument_group('optional arguments')
            for arg in optional_arguments:
                name = arg['name']
                del arg['name']
                try:
                    action_value = arg['action']
                    del arg['action']
                    optional.add_argument(f'--{name}', required=False, action=action_value, **arg)
                except:
                    optional.add_argument(f'--{name}', metavar='', required=False, **arg)

            optional.add_argument('-c', '--syntax-css', action='store', default='vs', help=argparse.SUPPRESS)
            optional.add_argument('-p', '--print-width', action='store_true', help=argparse.SUPPRESS)
            optional.add_argument('-v', '--verbose', action='store_true', help=argparse.SUPPRESS)

        return action

    @classmethod
    def get_args(cls):
        parser = argparse.ArgumentParser(prog='Akamai CLI utility',
                                         formatter_class=AkamaiParser,
                                         conflict_handler='resolve',
                                         usage='Various akamai utilities to facilitate day to day work')

        # parser.add_argument('-v', '--verbose', action='store_true', help='set log level to DEBUG')

        parser.add_argument('-a', '--account-key', '--account-switchkey', '--accountswitchkey',
                            metavar='', type=str, dest='account_switch_key',
                            help='account switch key (Akamai Internal Only)')
        parser.add_argument('-e', '--edgerc',
                            metavar='', type=str, dest='section',
                            help='location of the credentials file [$AKAMAI_EDGERC]')
        parser.add_argument('-s', '--section',
                            metavar='', type=str, dest='section',
                            help='section of the credentials file [$AKAMAI_EDGERC_SECTION]')

        subparsers = parser.add_subparsers(title='Available commands', metavar='', dest='command')

        # This is how available commands are displayed on terminal.
        # If you want diff to show before admin,
        # move actions['diff] before line actions['admin]
        actions = {}
        actions['search'] = cls.create_sub_command(
                            subparsers,
                            'search',
                            help='administrative lookup',
                            optional_arguments=[{'name': 'accounts', 'help': 'keyword search at least 3 characters', 'nargs': '+'}])

        diff_help = 'show compare report between two configurations. By default, configuration is compared using JSON.\nIf you want to compare metadata, add --xml'
        actions['diff'] = cls.create_sub_command(
                            subparsers,
                            'diff',
                            help=f'{diff_help}',
                            required_arguments=[{'name': 'config1', 'help': 'config to compare'}],
                            optional_arguments=[{'name': 'xml', 'help': 'compare metadata', 'action': 'store_true'},
                                                {'name': 'json', 'help': 'compare json', 'action': 'store_false'},
                                                {'name': 'config2', 'help': 'another config to be compared with config #1'},
                                                {'name': 'security', 'help': 'required argument if the comparison if for security config', 'action': 'store_true'},
                                                {'name': 'name-contains', 'help': 'security config name keyword'},
                                                {'name': 'left', 'help': 'config1 version'},
                                                {'name': 'right', 'help': 'config2 version'},
                                                {'name': 'no-show', 'help': 'automatically open compare report in browser', 'action': 'store_true'},
                                                {'name': 'acc-cookies', 'help': '3 cookies value from control.akamai.com'},
                                                {'name': 'remove-tags', 'help': 'ignore JSON/XML tags from comparison', 'nargs': '+'}
                                                ])

        config_help = 'many things you may need to (know about/check on/perform on) configs on the account'
        actions['delivery-config'] = cls.create_sub_command(
                            subparsers, 'delivery-config',
                            help=f'{config_help}',
                            optional_arguments=[{'name': 'activate', 'help': 'activate property version', 'action': 'store_true'},
                                                {'name': 'dryrun', 'help': 'verification only', 'action': 'store_true'},
                                                {'name': 'load', 'help': 'filename'},
                                                {'name': 'directory', 'help': 'directory where all CSVs are located'},
                                                {'name': 'ruletree', 'help': 'view ruletree structure format', 'action': 'store_true'},
                                                {'name': 'output', 'help': 'output filename.extension ie akamai.xlsx'},
                                                {'name': 'version', 'help': 'version to activate'},
                                                {'name': 'sheet', 'help': 'sheet name of the excel'},
                                                {'name': 'filter', 'help': 'lookup keyword'},
                                                {'name': 'show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'},
                                                {'name': 'network', 'help': 'options: staging, production', 'default': 'staging', 'nargs': '+'},
                                                {'name': 'note', 'help': 'activation note', 'nargs': '+'},
                                                {'name': 'emails', 'help': 'notificatin emails for activations', 'nargs': '+'},
                                                {'name': 'group-id', 'help': 'provide at least one group without prefix grp_ ', 'nargs': '+'},
                                                {'name': 'property-id', 'help': 'provide at least one property with out prefix prp_', 'nargs': '+'},
                                                ])

        actions['report'] = cls.create_sub_command(
                            subparsers, 'report',
                            help='list traffic report',
                            optional_arguments=[
                                {'name': 'url-offload', 'help': 'url hit offload', 'action': 'store_true'},
                                {'name': 'cpcodes', 'help': '1 or more reporting cpcodes ', 'nargs': '+'}
                            ])

        actions['ruleformat'] = cls.create_sub_command(
                            subparsers, 'ruleformat',
                            help='information about ruleformat and behavior catalog',
                            required_arguments=[{'name': 'product-id', 'help': 'product_id, https://techdocs.akamai.com/property-mgr/reference/id-prefixes#common-product-ids'}],
                            optional_arguments=[{'name': 'version', 'help': 'version, https://techdocs.akamai.com/property-mgr/reference/get-schemas-product-rule-format'},
                                                {'name': 'behavior', 'help': 'behavior names contain', 'nargs': '+'},
                                                {'name': 'xlsx', 'help': 'save XLSX file locally', 'action': 'store_true'},
                                                {'name': 'json', 'help': 'display JSON result to terminal', 'action': 'store_true'}])

        actions['log'] = cls.create_sub_command(
                            subparsers,
                            'log',
                            help='review ghost logs, excel friendly',
                            required_arguments=[{'name': 'input', 'help': 'location of file ending with gz extension'}],
                            optional_arguments=[{'name': 'output', 'help': 'location of excel file'},
                                                {'name': 'search', 'help': 'search text', 'nargs': '+'}])

        return parser.parse_args()
