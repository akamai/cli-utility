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
    def get_args(cls):
        parser = argparse.ArgumentParser(prog='Akamai CLI utility',
                                         formatter_class=AkamaiParser,
                                         conflict_handler='resolve',
                                         usage='Various akamai utilities to facilitate day to day work')

        parser.add_argument('-a', '--account-key', '--account-switchkey', '--accountswitchkey',
                            metavar='', type=str, dest='account_switch_key',
                            help='account switch key (Internal Only)')

        subparsers = parser.add_subparsers(title='Available commands', metavar='', dest='command')

        actions = {}
        actions['diff'] = cls.create_sub_command(subparsers,
                            'diff',
                            help='show compare report between two configurations',
                            required_arguments=[{'name': 'config1', 'help': 'config to compare'}],
                            optional_arguments=[{'name': 'config2', 'help': 'another config to be compared with config #1'},
                                                {'name': 'left', 'help': 'config1 version'},
                                                {'name': 'right', 'help': 'config2 version'},
                                                {'name': 'kind', 'help': 'delivery or security', 'default': 'delivery'},
                                                {'name': 'xml', 'help': 'compare metadata', 'action': 'store_false'},
                                                {'name': 'no-show', 'help': 'automatically open compare report in browser', 'action': 'store_true'},
                                                {'name': 'remove-tags', 'help': 'ignore json tags from comparison', 'nargs': '+'}
                                                ])
        actions['delivery-config'] = cls.create_sub_command(subparsers, 'delivery-config', help='collect all configs for account')
        actions['report'] = cls.create_sub_command(subparsers, 'report', help='list traffic report')
        actions['ruleformat'] = cls.create_sub_command(subparsers, 'ruleformat', help='download ruleformat version',
                                                        required_arguments=[{'name': 'product-id', 'help': 'product_id, https://techdocs.akamai.com/property-mgr/reference/id-prefixes'}],
                                                        optional_arguments=[{'name': 'version', 'help': 'version, https://techdocs.akamai.com/property-mgr/reference/get-schemas-product-rule-format'},
                                                                            {'name': 'behavior', 'help': 'behavior name', 'nargs': '+'},
                                                                            {'name': 'save', 'help': 'save JSON file locally', 'action': 'store_true'},
                                                                            {'name': 'xlsx', 'help': 'save XLSX file locally', 'action': 'store_true'},
                                                                            {'name': 'json', 'help': 'display JSON result to terminal', 'action': 'store_true'}])

        return parser.parse_args()

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
