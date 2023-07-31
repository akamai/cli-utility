from __future__ import annotations

import argparse
import sys

import rich_argparse as rap


class OnelineArgumentFormatter(rap.ArgumentDefaultsRichHelpFormatter):
    def __init__(self, prog, max_help_position=30, **kwargs):
        super().__init__(prog, **kwargs)
        self._max_help_position = max_help_position

    def print_usage(self, file=None):
        if file is None:
            file = sys.stdout
        self._print_message(self.usage, file, False)

    def _format_usage(self, usage, actions, groups, prefix):
        # Do not include the default usage line
        return 'Usage:'


class CustomHelpFormatter(rap.RichHelpFormatter):
    def __init__(self, prog, indent_increment=2, max_help_position=30, width=None):
        super().__init__(prog, indent_increment, max_help_position, width)


class AkamaiParser(CustomHelpFormatter, argparse.ArgumentParser):
    def __init__(self, prog):
        super().__init__(prog,
                         max_help_position=30)

    def format_usage(self):
        # Check if the current command is a subcommand
        is_subcommand = self._is_subcommand()

        # Return the default usage for subcommands
        if is_subcommand:
            return ''

        # Return the original usage for the main command
        return super().format_usage()

    def _is_subcommand(self):
        # Check if the current command is a subcommand by examining the presence of subparsers
        return hasattr(self, '_subparsers') and self._subparsers is not None

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

        report_sc = [{'name': 'list',
                      'help': 'list all available reports',
                      'optional_arguments': [{'name': 'type', 'help': 'Options: account_id, contractId, contracts, cpcode, edns, fpdomain, reportPackId',
                                              'choices': ['account_id', 'contractId', 'contracts', 'cpcode', 'edns', 'fpdomain', 'reportPackId']},
                                             {'name': 'namecontains', 'help': 'report name contains'}]},
                     {'name': 'url-offload',
                      'help': 'list report by url',
                      'optional_arguments': [{'name': 'cpcode', 'help': '1 or more reporting cpcodes ', 'nargs': '+'}]},
                     {'name': 'response-class',
                      'help': 'list report by response codes',
                      'optional_arguments': [{'name': 'cpcode', 'help': '1 or more reporting cpcodes ', 'nargs': '+'},
                                             {'name': 'interval', 'help': 'The duration of each data record',
                                              'choices': ['MONTH', 'WEEK', 'DAY', 'HOUR', 'FIVE_MINUTES'],
                                              'default': 'HOUR'},
                                             {'name': 'file', 'help': 'input file you stores one cpcode per line'},
                                             {'name': 'ratelimit', 'help': 'rate limit [numeric]', 'default': 5},
                                             {'name': 'concurrency', 'help': 'process X [numeric] requests at a time', 'default': 2},
                                             {'name': 'timeout', 'help': 'http timeout in second [numeric]', 'default': 15},
                                             {'name': 'sample', 'help': 'sample size [numeric]', 'default': None},
                                             {'name': 'output', 'help': 'json or csv', 'default': 'json'}]}
                     ]
        actions['report'] = cls.create_main_command(
                            subparsers, 'report',
                            help='list traffic report',
                            subcommands=report_sc,
                            options=None)

        actions['gtm'] = cls.create_main_command(
                            subparsers, 'gtm',
                            help='list GTM report',
                            optional_arguments=[
                                {'name': 'output', 'help': 'output filename.extension ie akamai.xlsx'},
                                {'name': 'no-show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'}])

        actions['certificate'] = cls.create_main_command(
                            subparsers, 'certificate',
                            help='certificate report includes enrollmentId, slotId, SNI, hostname, commonName, cName, vendor, expirationDate',
                            optional_arguments=[
                                {'name': 'expire', 'help': 'only show expired certificate', 'action': 'store_true'},
                                {'name': 'output', 'help': 'output filename.extension ie akamai.xlsx'},
                                {'name': 'sni', 'help': 'only show SNI deployement type', 'action': 'store_true'},
                                {'name': 'show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'},
                                {'name': 'contract-id', 'help': 'provide at least one contractId without prefix crt_ ', 'nargs': '+'},
                                {'name': 'enrollment-id', 'help': 'provide at least one enrollment id', 'nargs': '+'},
                                {'name': 'slot', 'help': 'provide at least one slot id', 'nargs': '+'},
                                {'name': 'authority', 'help': 'certificate authority',
                                 'choices': ['lets-encrypt', 'symantec', 'third-party', 'geotrust'], 'nargs': '+'}
                            ])

        config_help = 'many things you may need to (know about/check on/perform on) configs on the account'
        dc_sc = [{'name': 'behavior',
                  'help': 'list all behaviors on the property',
                  'required_arguments': [{'name': 'property', 'help': 'property name'}],
                  'optional_arguments': [{'name': 'version', 'help': 'version'},
                                         {'name': 'remove-tag', 'help': 'ignore JSON/XML tags from comparison', 'nargs': '+'}]},
                 {'name': 'custom-behavior',
                  'help': 'list custom behavior on the account',
                  'optional_arguments': [{'name': 'id', 'help': 'behaviorId', 'nargs': '+'},
                                         {'name': 'namecontains', 'help': 'behavior name contains keyword search'},
                                         {'name': 'hidexml', 'help': 'use this argument to hide XML result from the terminal', 'action': 'store_false'},
                                         {'name': 'lineno', 'help': 'show line number', 'action': 'store_true'}]},
                 {'name': 'metadata',
                  'help': 'view XML for all advanced metadata including advanced behavior, advanced match, and advanced override for the properties',
                  'required_arguments': [{'name': 'property', 'help': 'property name', 'nargs': '+'}],
                  'optional_arguments': [{'name': 'version', 'help': 'property version'},
                                         {'name': 'hidexml', 'help': 'use this argument to hide XML result from the terminal', 'action': 'store_false'},
                                         {'name': 'lineno', 'help': 'show line number', 'action': 'store_true'},
                                         {'name': 'no-show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'}]},
                  {'name': 'hostname-cert',
                  'help': 'Show if property hostnames use Security By Default or Certificate managed in CPS.',
                  'required_arguments': [{'name': 'property', 'help': 'property name', 'nargs': '+'}],
                  'optional_arguments': [{'name': 'version', 'help': 'version'},
                                         {'name': 'no-show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'}]},
                  {'name': 'netstorage',
                  'help': 'get detail of net storage on property',
                  'optional_arguments': [{'name': 'group-id', 'help': 'provide at least one groupId without prefix grp_ ', 'nargs': '+'},
                                         {'name': 'property', 'help': 'provide at least one property name ', 'nargs': '+'},
                                         {'name': 'output', 'help': 'output filename.extension ie akamai.xlsx'},
                                         {'name': 'show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'}]},
                  {'name': 'origin-cert',
                  'help': 'check certificate for all origin servers on property',
                  'optional_arguments': [{'name': 'group-id', 'help': 'provide at least one groupId without prefix grp_ ', 'nargs': '+'},
                                         {'name': 'property', 'help': 'provide at least one property name ', 'nargs': '+'},
                                         {'name': 'output', 'help': 'output filename.extension ie akamai.xlsx'},
                                         {'name': 'show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'}]},
                 {'name': 'ruletree',
                  'help': 'view ruletree structure format',
                  'required_arguments': [{'name': 'property', 'help': 'property name', 'nargs': '+'}],
                  'optional_arguments': [{'name': 'version', 'help': 'version'},
                                         {'name': 'show-depth', 'help': 'to display max depth', 'action': 'store_true'},
                                         {'name': 'show-limit', 'help': 'show config limit ie. max-nested-rules-limit', 'action': 'store_true'}]},
                 {'name': 'activate',
                  'help': 'activate property',
                  'required_arguments': [{'name': 'file', 'help': 'excel file'},
                                         {'name': 'email', 'help': 'notificatin email(s) for activations', 'nargs': '+'},
                                         {'name': 'network', 'help': 'options: staging, production', 'default': 'staging', 'nargs': '+'},
                                         {'name': 'note', 'help': 'activation note', 'nargs': '+'}],
                  'optional_arguments': [{'name': 'sheet', 'help': 'excel sheetname'},
                                         {'name': 'filter-column', 'help': 'filter column of the excel table'},
                                         {'name': 'property-id', 'help': 'provide at least one property without prefix prp_', 'nargs': '+'}]}
                ]

        actions['delivery-config'] = cls.create_main_command(
            subparsers, 'delivery-config', help=f'{config_help}',
            optional_arguments=[{'name': 'summary', 'help': 'only show account summary', 'action': 'store_true'},
                                {'name': 'output', 'help': 'output filename.extension ie akamai.xlsx'},
                                {'name': 'show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'},
                                {'name': 'behavior', 'help': 'behaviors you want to audit on the property', 'nargs': '+'},
                                {'name': 'group-id', 'help': 'provide at least one groupId without prefix grp_ ', 'nargs': '+'},
                                {'name': 'property', 'help': 'provide at least one propertyId without prefix prp_ ', 'nargs': '+'},
                                ],
            subcommands=dc_sc,
            options=None)

        actions['ruleformat'] = cls.create_main_command(
                            subparsers, 'ruleformat',
                            help='information about ruleformat and behavior catalog',
                            required_arguments=[{'name': 'product-id',
                                                 'choices': ['prd_Site_Accel', 'prd_Fresca', 'prd_SPM',
                                                             'prd_Site_Del', 'prd_Rich_Media_Accel', 'prd_IoT',
                                                             'prd_Site_Defender',
                                                             'prd_Download_Delivery', 'prd_Object_Delivery',
                                                             'prd_Adaptive_Media_Delivery'],
                                                 'help': 'product_id, https://techdocs.akamai.com/property-mgr/reference/id-prefixes#common-product-ids'}],
                            optional_arguments=[{'name': 'version', 'help': 'version, https://techdocs.akamai.com/property-mgr/reference/get-schemas-product-rule-format'},
                                                {'name': 'nameonly', 'help': 'only show behavior name', 'action': 'store_true'},
                                                {'name': 'behavior', 'help': 'behavior names contain', 'nargs': '+'},
                                                {'name': 'xlsx', 'help': 'save XLSX file locally', 'action': 'store_true'},
                                                {'name': 'sample', 'help': 'show json with sample data', 'action': 'store_true'},
                                                {'name': 'json', 'help': 'display result in JSON format', 'action': 'store_true'},
                                                {'name': 'table', 'help': 'display result as table', 'action': 'store_true'}
                                                ])

        actions['log'] = cls.create_main_command(
                            subparsers,
                            'log',
                            help='review ghost logs, excel friendly',
                            required_arguments=[{'name': 'input', 'help': 'location of file ending with gz extension'}],
                            optional_arguments=[{'name': 'output', 'help': 'location of excel file'},
                                                {'name': 'only', 'help': 'R or F', 'choices': ['R', 'F'], 'default': 'R'},
                                                {'name': 'column', 'help': 'filter column'},
                                                {'name': 'value-contains', 'help': 'search text', 'nargs': '+'}])

        actions['search'] = cls.create_main_command(
                            subparsers,
                            'search',
                            help='administrative lookup',
                            optional_arguments=[{'name': 'account', 'help': 'keyword search at least 3 characters', 'nargs': '+'},
                                                {'name': 'accountkey', 'help': argparse.SUPPRESS}])

        sec_sc = [{'name': 'hostname',
                  'help': 'audit hostnames not yet assigned to the security configurations',
                  'optional_arguments': [{'name': 'group-id', 'help': 'group-id', 'nargs': '+'},
                                         {'name': 'output', 'help': 'override excel output file (.xlsx)'},
                                         {'name': 'no-show', 'help': 'automatically open excel', 'action': 'store_true'}]
                  }]
        actions['security'] = cls.create_main_command(
                            subparsers,
                            'security',
                            help='collect detail about security configuration',
                            subcommands=sec_sc,
                            required_arguments=[{'name': 'config', 'help': 'security config name', 'nargs': '+'}],
                            optional_arguments=[{'name': 'version', 'help': 'security config version'},
                                                {'name': 'group-id', 'help': 'group-id', 'nargs': '+'},
                                                {'name': 'output', 'help': 'override excel output file (.xlsx)'},
                                                {'name': 'no-show', 'help': 'automatically open compare report in browser', 'action': 'store_true'}])

        diff_help = 'show compare report between two configurations. By default, configuration is compared using JSON.\nIf you want to compare metadata, add --xml'
        diff_sc = [{'name': 'behavior',
                  'help': 'compare behavior between two delivery configurations',
                  'required_arguments': [{'name': 'property', 'help': 'propertyname', 'nargs': '+'}],
                  'optional_arguments': [{'name': 'behavior', 'help': 'behavior', 'nargs': '+'},
                                         {'name': 'criteria', 'help': 'criteria', 'nargs': '+'},
                                         {'name': 'rulecontains', 'help': 'include rules contains', 'nargs': '+'},
                                         {'name': 'rulenotcontains', 'help': 'exclude rules contains', 'nargs': '+'},
                                         {'name': 'remove-tag', 'help': 'ignore JSON/XML tags from comparison', 'nargs': '+'},
                                         {'name': 'output', 'help': 'override excel output file (.xlsx)'},
                                         {'name': 'no-show', 'help': 'automatically open compare report in browser', 'action': 'store_true'}]
                  }]

        actions['diff'] = cls.create_main_command(
                            subparsers,
                            'diff',
                            help=f'{diff_help}',
                            required_arguments=[{'name': 'config1', 'help': 'config to compare'}],
                            optional_arguments=[{'name': 'xml', 'help': 'compare metadata', 'action': 'store_true'},
                                                {'name': 'json', 'help': 'compare json', 'action': 'store_false'},
                                                {'name': 'config2', 'help': 'another config to be compared with config #1'},
                                                {'name': 'security', 'help': 'required argument if the comparison if for security config', 'action': 'store_true'},
                                                {'name': 'namecontains', 'help': 'security config name keyword'},
                                                {'name': 'left', 'help': 'config1 version'},
                                                {'name': 'right', 'help': 'config2 version'},
                                                {'name': 'no-show', 'help': 'automatically open compare report in browser', 'action': 'store_true'},
                                                {'name': 'acc-cookies', 'help': '3 cookies value from control.akamai.com'},
                                                {'name': 'remove-tag', 'help': 'ignore JSON/XML tags from comparison', 'nargs': '+'}
                                                ],
                            subcommands=diff_sc,
                            options=None)

        return parser.parse_args()

    @classmethod
    def create_main_command(cls, subparsers, name, help,
                            required_arguments=None,
                            optional_arguments=None,
                            subcommands=None,
                            options=None):

        action = subparsers.add_parser(name=name,
                                       help=help,
                                       add_help=True,
                                       formatter_class=OnelineArgumentFormatter)
        action.description = help  # Set the subcommand's help message as the description
        action.usage = f'%(prog)s {name} [options]'  # Set a custom usage format

        if subcommands:
            subparsers = action.add_subparsers(title=name, metavar='', dest='subcommand')
            for subcommand in subcommands:
                subcommand_name = subcommand['name']
                subcommand_help = subcommand['help']
                subcommand_required = subcommand.get('required_arguments', None)
                subcommand_optional = subcommand.get('optional_arguments', None)
                cls.create_main_command(subparsers, subcommand_name, subcommand_help,
                                        subcommand_required,
                                        subcommand_optional,
                                        subcommands=subcommand.get('subcommands', None))

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
    def add_mutually_exclusive_group(cls, action, argument, conflicting_argument):

        group = action.add_mutually_exclusive_group()
        group.add_argument(argument['name'], help=argument['help'], nargs='+')

        # Add the conflicting argument to the group as a mutually exclusive argument
        conflicting_argument_help = [arg['help'] for arg in argument if arg['name'] == conflicting_argument]
        group.add_argument(conflicting_argument, help=conflicting_argument_help, nargs='+')

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
                    required.add_argument(f'--{name}', action=action_value, **arg)
                except KeyError:
                    required.add_argument(f'--{name}', metavar='', **arg)

        if optional_arguments:
            optional = action.add_argument_group('Optional Arguments')
            for arg in optional_arguments:
                if arg['name'] == '--group-id':
                    cls.add_mutually_exclusive_group(action, arg, '--property-id')
                elif arg['name'] == '--property-id':
                    cls.add_mutually_exclusive_group(action, arg, '--group-id')
                else:
                    name = arg['name']
                    del arg['name']
                    try:
                        action_value = arg['action']
                        del arg['action']
                        optional.add_argument(f'--{name}', required=False, action=action_value, **arg)
                    except KeyError:
                        optional.add_argument(f'--{name}', metavar='', required=False, **arg)

            optional.add_argument('-c', '--syntax-css', action='store', default='vs', help=argparse.SUPPRESS)
            optional.add_argument('-p', '--print-width', action='store_true', help=argparse.SUPPRESS)
            optional.add_argument('-v', '--verbose', action='store_true', help=argparse.SUPPRESS)
            optional.add_argument('--log-level',
                                  choices=['debug', 'info', 'warning', 'error', 'critical'],
                                  default='warning',
                                  help='Set the log level',
                                 )
