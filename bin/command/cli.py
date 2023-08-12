from __future__ import annotations


delivery = [{'name': 'behavior',
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
                                         {'name': 'concurrency', 'help': 'increase concurrency to X.  Maximum value is 10.', 'default': 1},
                                         {'name': 'show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'}]},
                  {'name': 'origin-cert',
                  'help': 'check certificate for all origin servers on property',
                  'optional_arguments': [{'name': 'group-id', 'help': 'provide at least one groupId without prefix grp_ ', 'nargs': '+'},
                                         {'name': 'property', 'help': 'provide at least one property name ', 'nargs': '+'},
                                         {'name': 'output', 'help': 'output filename.extension ie akamai.xlsx'},
                                         {'name': 'concurrency', 'help': 'increase concurrency to X.  Maximum value is 10.', 'default': 1},
                                         {'name': 'show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'}]},
                 {'name': 'ruletree',
                  'help': 'view ruletree structure format',
                  'required_arguments': [{'name': 'property', 'help': 'property name', 'nargs': '+'}],
                  'optional_arguments': [{'name': 'version', 'help': 'version'},
                                         {'name': 'show-depth', 'help': 'to display max depth', 'action': 'store_true'},
                                         {'name': 'show-limit', 'help': 'show config limit ie. max-nested-rules-limit', 'action': 'store_true'}]},
                 {'name': 'jsonpath',
                  'help': 'view jsonpath for behavior and criteria on the property',
                  'required_arguments': [{'name': 'property', 'help': 'property name', 'nargs': '+'},
                                         {'name': 'type', 'help': 'Options: criteria, behavior', 'nargs': '+',
                                          'choices': ['behavior', 'criteria'],
                                          'default': ['behavior', 'criteria']}],
                  'optional_arguments': [{'name': 'version', 'help': 'version'},
                                         {'name': 'criteria', 'help': 'criteria name', 'nargs': '+'},
                                         {'name': 'behavior', 'help': 'behavior name', 'nargs': '+'},
                                         {'name': 'rulecontains', 'help': 'include rules contains', 'nargs': '+'}]},
                ]

security = [{'name': 'hostname',
             'help': 'audit hostnames not yet assigned to the security configurations',
             'optional_arguments': [{'name': 'group-id', 'help': 'group-id', 'nargs': '+'},
                                    {'name': 'concurrency', 'help': 'process X [numeric] requests at a time.  Maximum value is 10.', 'default': 1},
                                    {'name': 'output', 'help': 'override excel output file (.xlsx)'},
                                    {'name': 'no-show', 'help': 'automatically open excel', 'action': 'store_true'}]
             }]

diff = [{'name': 'behavior',
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

gtm = [{'name': 'remove',
        'help': 'removes specific GTM properties listed in the csv input file',
        'required_arguments': [{'name': 'input', 'help': 'csv input file with 2 headers called domain,property'}]}]

report = [{'name': 'list',
           'help': 'list all available reports',
           'optional_arguments': [{'name': 'type', 'help': 'Options: account_id, contractId, contracts, cpcode, edns, fpdomain, reportPackId',
                                   'choices': ['account_id', 'contractId', 'contracts', 'cpcode', 'edns', 'fpdomain', 'reportPackId']},
                                  {'name': 'namecontains', 'help': 'report name contains'}]},
          {'name': 'offload-file-extension', 'help': 'file extension offload report',
           'required_arguments': [{'name': 'last', 'help': 'last X interval', 'default': 7},
                                  {'name': 'interval', 'help': 'Options: MONTH, WEEK, DAY, HOUR, FIVE_MINUTES',
                                   'choices': ['MONTH', 'WEEK', 'DAY', 'HOUR', 'FIVE_MINUTES'],
                                   'default': 'HOUR'}],
           'optional_arguments': [{'name': 'cpcode', 'help': '1 or more reporting cpcodes ', 'nargs': '+'}]},
          {'name': 'offload-hostname', 'help': 'traffic report by hostname',
           'required_arguments': [{'name': 'last', 'help': 'last X interval', 'default': 7},
                                  {'name': 'interval', 'help': 'Options: MONTH, WEEK, DAY, HOUR, FIVE_MINUTES',
                                   'choices': ['MONTH', 'WEEK', 'DAY', 'HOUR', 'FIVE_MINUTES'],
                                   'default': 'HOUR'}]},
          {'name': 'response-class', 'help': 'traffic report by response codes class',
           'required_arguments': [{'name': 'last', 'help': 'last X interval', 'default': 7},
                                  {'name': 'interval', 'help': 'Options: MONTH, WEEK, DAY, HOUR, FIVE_MINUTES',
                                   'choices': ['MONTH', 'WEEK', 'DAY', 'HOUR', 'FIVE_MINUTES'],
                                   'default': 'HOUR'}],
           'optional_arguments': [{'name': 'cpcode', 'help': '1 or more reporting cpcodes ', 'nargs': '+'},
                                  {'name': 'file', 'help': 'input file you stores one cpcode per line'},
                                  {'name': 'ratelimit', 'help': 'rate limit [numeric]', 'default': 5},
                                  {'name': 'concurrency', 'help': 'process X [numeric] requests at a time.  Maximum value is 10.', 'default': 2},
                                  {'name': 'timeout', 'help': 'http timeout in second [numeric]', 'default': 15},
                                  {'name': 'sample', 'help': 'sample size [numeric]', 'default': None},
                                  {'name': 'output', 'help': 'json or csv', 'default': 'json'}]}
         ]

sub_commands = {'delivery': delivery,
                'security': security,
                'diff': diff,
                'gtm': gtm,
                'report': report,
                }

main_commands = [{'delivery': 'information detail about delivery configuration',
                  'optional_arguments': [{'name': 'summary', 'help': 'only show account summary', 'action': 'store_true'},
                                         {'name': 'output', 'help': 'output filename.extension ie akamai.xlsx'},
                                         {'name': 'concurrency', 'help': 'increase concurrency to X.  Maximum value is 10.', 'default': 1},
                                         {'name': 'show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'},
                                         {'name': 'behavior', 'help': 'behaviors you want to audit on the property', 'nargs': '+'},
                                         {'name': 'group-id', 'help': 'provide at least one groupId without prefix grp_ ', 'nargs': '+'},
                                         {'name': 'property', 'help': 'provide at least one propertyId without prefix prp_ ', 'nargs': '+'}]},
                 {'security': 'information detail about security configuration',
                  'required_arguments': [{'name': 'config', 'help': 'security config name', 'nargs': '+'}],
                  'optional_arguments': [{'name': 'version', 'help': 'security config version'},
                                         {'name': 'group-id', 'help': 'group-id', 'nargs': '+'},
                                         {'name': 'output', 'help': 'override excel output file (.xlsx)'},
                                         {'name': 'no-show', 'help': 'automatically open compare report in browser',
                                          'action': 'store_true'}]},
                 {'diff': 'show compare report between two configurations. Both delivery and security configs are supported',
                  'required_arguments': [{'name': 'config1', 'help': 'config to compare', 'required': False}],
                  'optional_arguments': [{'name': 'xml', 'help': 'compare metadata', 'action': 'store_true'},
                                         {'name': 'json', 'help': 'compare json', 'action': 'store_false'},
                                         {'name': 'config2', 'help': 'another config to be compared with config #1'},
                                         {'name': 'security', 'help': 'required argument if the comparison if for security config', 'action': 'store_true'},
                                         {'name': 'namecontains', 'help': 'security config name keyword'},
                                         {'name': 'left', 'help': 'config1 version'},
                                         {'name': 'right', 'help': 'config2 version'},
                                         {'name': 'no-show', 'help': 'automatically open compare report in browser', 'action': 'store_true'},
                                         {'name': 'acc-cookies', 'help': '3 cookies value from control.akamai.com'},
                                         {'name': 'remove-tag', 'help': 'ignore JSON/XML tags from comparison', 'nargs': '+'}
                                         ]},
                 {'certificate': 'certificate report includes enrollmentId, slotId, SNI, hostname, commonName, cName, vendor, expirationDate',
                  'optional_arguments': [{'name': 'expire', 'help': 'only show expired certificate', 'action': 'store_true'},
                                         {'name': 'sni', 'help': 'only show SNI deployement type', 'action': 'store_true'},
                                         {'name': 'contract-id', 'help': 'provide at least one contractId without prefix ctr_ ', 'nargs': '+'},
                                         {'name': 'enrollment-id', 'help': 'provide at least one enrollment id', 'nargs': '+'},
                                         {'name': 'slot', 'help': 'provide at least one slot id', 'nargs': '+'},
                                         {'name': 'authority', 'help': 'certificate authority ie. lets-encrypt, symantec, third-party',
                                          'choices': ['lets-encrypt', 'symantec', 'third-party', 'geotrust'], 'nargs': '+'},
                                         {'name': 'output', 'help': 'xlsx output file', 'default': 'certificate.xlsx'},
                                         {'name': 'show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'}]},
                 {'gtm': 'list GTM report',
                  'optional_arguments': [{'name': 'output', 'help': 'output filename.extension ie akamai.xlsx'},
                                         {'name': 'no-show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'}]},
                 {'report': 'available reports via API'},
                 {'ruleformat': 'JSON ruleformat and behavior catalog',
                  'required_arguments': [{'name': 'product-id',
                                          'help': 'Akamai productId starts with prefix prd_',
                                          'default': 'prd_SPM',
                                          'required': False,
                                          'choices': ['prd_Site_Accel', 'prd_Fresca', 'prd_SPM',
                                                      'prd_Site_Del', 'prd_Rich_Media_Accel', 'prd_IoT',
                                                      'prd_Site_Defender',
                                                      'prd_Download_Delivery', 'prd_Object_Delivery',
                                                      'prd_Adaptive_Media_Delivery']}],
                  'optional_arguments': [{'name': 'version', 'help': 'version'},
                                         {'name': 'nameonly', 'help': 'only show behavior name', 'action': 'store_true'},
                                         {'name': 'behavior', 'help': 'behavior names contain', 'nargs': '+'},
                                         {'name': 'xlsx', 'help': 'save XLSX file locally', 'action': 'store_true'},
                                         {'name': 'sample', 'help': 'show json with sample data', 'action': 'store_true'},
                                         {'name': 'json', 'help': 'display result in JSON format', 'action': 'store_true'},
                                         {'name': 'table', 'help': 'display result as table', 'action': 'store_true'}
                                         ]},
                 {'log': 'review ghost logs, excel friendly (internal to Akamai employees only)',
                  'required_arguments': [{'name': 'input', 'help': 'location of file ending with gz extension', 'required': False}],
                  'optional_arguments': [{'name': 'output', 'help': 'location of excel file'},
                                         {'name': 'only', 'help': 'R or F', 'choices': ['R', 'F'], 'default': 'R'},
                                         {'name': 'column', 'help': 'filter column'},
                                         {'name': 'value-contains', 'help': 'search text', 'nargs': '+'}]},
                 {'search': 'lookup account key/account name',
                  'required_arguments': [{'name': 'account',
                                          'help': 'keyword search at least 3 characters', 'nargs': '+',
                                          'required': True}]},
                ]