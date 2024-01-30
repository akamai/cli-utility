from __future__ import annotations

import pandas as pd
from akamai_utils import cpcode as cp
from utils import files


def list_cpcode(args, account_folder, logger):
    cpc = cp.CpCodeWrapper(account_switch_key=args.account_switch_key,
                           section=args.section,
                           edgerc=args.edgerc,
                           logger=logger)
    resp = cpc.list_cpcode()

    logger.info(f"Total cpcodes: {len(resp['cpcodes'])}")
    df = pd.DataFrame(resp['cpcodes'])
    df.index = df.index + 1
    columns = ['cpcodeId', 'cpcodeName', 'contracts', 'products', 'accountId']

    sheet = {}
    sheet['all_cpcodes'] = df[columns]
    filepath = f'{account_folder}/all_cpcodes.xlsx'
    files.write_xlsx(filepath, sheet, freeze_column=1) if not df.empty else None
    files.open_excel_application(filepath, True, df)
