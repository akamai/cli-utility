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


class OnelineArgumentFormatter(rap.ArgumentDefaultsRichHelpFormatter):
    def __init__(self, prog, max_help_position=30, **kwargs):
        super().__init__(prog, **kwargs)
        self._max_help_position = max_help_position


class AkamaiParser(rap.RichHelpFormatter, argparse.HelpFormatter, argparse.ArgumentParser):
    def __init__(self, prog):
        super().__init__(prog,
                         max_help_position=30)

    @classmethod
    def create_main_command(cls, subparsers, name, help, required_arguments=None, optional_arguments=None, subcommands=None, options=None):
        action = subparsers.add_parser(name=name, help=help, add_help=True, formatter_class=OnelineArgumentFormatter)

        if subcommands:
            subparsers = action.add_subparsers(title='Available Commands', metavar='', dest='subcommand')
            for subcommand in subcommands:
                subcommand_name = subcommand['name']
                subcommand_help = subcommand['help']
                subcommand_required = subcommand.get('required_arguments', None)
                subcommand_optional = subcommand.get('optional_arguments', None)
                cls.create_main_command(subparsers, subcommand_name, subcommand_help, subcommand_required, subcommand_optional, subcommands=subcommand.get('subcommands', None))

        cls.add_arguments(action, required_arguments, optional_arguments)

        if options:
            options_group = action.add_argument_group('Options')
            for option in options:
                option_name = option['name']
                del option['name']
                try:
                    action_value = option['action']
                    del option['action']
                    options_group.add_argument(f'--{option_name}', action=action_value, **option)
                except KeyError:
                    options_group.add_argument(f'--{option_name}', metavar='', **option)
        return action

    @classmethod
    def get_args(cls):
        parser = argparse.ArgumentParser(prog='Akamai CLI utility',
                                         formatter_class=AkamaiParser,
                                         conflict_handler='resolve', add_help=True,
                                         usage='Various akamai utilities to facilitate day to day work')

        parser.add_argument('-a', '--accountkey',
                            metavar='accountkey', type=str, dest='account_switch_key',
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

        actions['report'] = cls.create_main_command(
                            subparsers, 'report',
                            help='list traffic report',
                            optional_arguments=[
                                {'name': 'url-offload', 'help': 'url hit offload', 'action': 'store_true'},
                                {'name': 'cpcodes', 'help': '1 or more reporting cpcodes ', 'nargs': '+'}
                            ])

        config_help = 'many things you may need to (know about/check on/perform on) configs on the account'
        dc_sc = [{'name': 'advancedmetadata',
                  'help': 'view XML for all advanced metadata',
                  'required_arguments': [{'name': 'property-id', 'help': 'propertyId', 'nargs': '+'},
                                         {'name': 'version', 'help': 'property version'}],
                  'optional_arguments': [{'name': 'filter', 'help': 'rulename keyword search', 'nargs': '+'},
                                         {'name': 'Match', 'help': 'Match', 'action': 'store_true'},
                                         {'name': 'Behavior', 'help': 'Behavior', 'action': 'store_true'},
                                         {'name': 'Override', 'help': 'Override', 'action': 'store_true'},
                                         {'name': 'noxml', 'help': 'use this argument to hide XML from the terminal', 'action': 'store_false'}]},
                 {'name': 'activate',
                  'help': 'activate property',
                  'required_arguments': [{'name': 'email', 'help': 'notificatin email(s) for activations', 'nargs': '+'},
                                         {'name': 'network', 'help': 'options: staging, production', 'default': 'staging', 'nargs': '+'},
                                         {'name': 'property-id', 'help': 'provide at least one property with out prefix prp_', 'nargs': '+'},
                                         {'name': 'note', 'help': 'activation note', 'nargs': '+'}]},
                 {'name': 'ruletree',
                  'help': 'view ruletree structure format',
                  'required_arguments': [{'name': 'property-id', 'help': 'provide at least one property with out prefix prp_', 'nargs': '+'}],
                  'optional_arguments': [{'name': 'version', 'help': 'version'}]}
                ]

        actions['delivery-config'] = cls.create_main_command(
            subparsers, 'delivery-config', help=f'{config_help}',
            optional_arguments=[{'name': 'directory', 'help': 'directory where all CSVs are located'},
                                {'name': 'input', 'help': 'input excel file'},
                                {'name': 'output', 'help': 'output filename.extension ie akamai.xlsx'},
                                {'name': 'sheet', 'help': 'sheet name of the excel'},
                                {'name': 'filter', 'help': 'lookup keyword'},
                                {'name': 'show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'},
                                {'name': 'behavior', 'help': 'behaviors you want to audit on the property', 'nargs': '+'},
                                {'name': 'group-id', 'help': 'provide at least one group without prefix grp_ ', 'nargs': '+'},
                                {'name': 'column', 'help': 'show column in excel output file ie url propertyId groupId contractId productId ruleFormat', 'nargs': '+'},
                                ],
            subcommands=dc_sc,
            options=None)

        actions['ruleformat'] = cls.create_main_command(
                            subparsers, 'ruleformat',
                            help='information about ruleformat and behavior catalog',
                            required_arguments=[{'name': 'product-id', 'choices': ['prd_Site_Accel', 'prd_Fresca', 'prd_SPM',
                                                                                   'prd_Site_Del', 'prd_Rich_Media_Accel', 'prd_IoT',
                                                                                   'prd_Site_Defender',
                                                                                   'prd_Download_Delivery', 'prd_Object_Delivery',
                                                                                   'prd_Adaptive_Media_Delivery'],
                                                 'help': 'product_id, https://techdocs.akamai.com/property-mgr/reference/id-prefixes#common-product-ids'}],
                            optional_arguments=[{'name': 'version', 'help': 'version, https://techdocs.akamai.com/property-mgr/reference/get-schemas-product-rule-format'},
                                                {'name': 'behavior', 'help': 'behavior names contain', 'nargs': '+'},
                                                {'name': 'show-behavior', 'help': 'only show behavior name', 'action': 'store_true'},
                                                {'name': 'xlsx', 'help': 'save XLSX file locally', 'action': 'store_true'},
                                                {'name': 'json', 'help': 'display JSON result to terminal', 'action': 'store_true'}])

        actions['log'] = cls.create_main_command(
                            subparsers,
                            'log',
                            help='review ghost logs, excel friendly',
                            required_arguments=[{'name': 'input', 'help': 'location of file ending with gz extension'}],
                            optional_arguments=[{'name': 'output', 'help': 'location of excel file'},
                                                {'name': 'search', 'help': 'search text', 'nargs': '+'}])

        actions['search'] = cls.create_main_command(
                            subparsers,
                            'search',
                            help='administrative lookup',
                            optional_arguments=[{'name': 'account', 'help': 'keyword search at least 3 characters', 'nargs': '+'},
                                                {'name': 'accountkey', 'help': argparse.SUPPRESS}])

        diff_help = 'show compare report between two configurations. By default, configuration is compared using JSON.\nIf you want to compare metadata, add --xml'
        actions['diff'] = cls.create_main_command(
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

        return parser.parse_args()

    @classmethod
    def add_arguments(cls, action, required_arguments=None, optional_arguments=None):
        if required_arguments:
            required = action.add_argument_group('Required Arguments')
            for arg in required_arguments:
                name = arg['name']
                del arg['name']
                try:
                    action_value = arg['action']
                    del arg['action']
                    required.add_argument(f'--{name}', required=True, action=action_value, **arg)
                except KeyError:
                    required.add_argument(f'--{name}', metavar='', required=True, **arg)

        if optional_arguments:
            optional = action.add_argument_group('Optional Arguments')
            for arg in optional_arguments:
                name = arg['name']
                del arg['name']
                try:
                    action_value = arg['action']
                    del arg['action']
                    optional.add_argument(f'--{name}', required=False, action=action_value, **arg)
                except KeyError:
                    optional.add_argument(f'--{name}', metavar='', required=False, **arg)
