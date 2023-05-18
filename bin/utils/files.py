from __future__ import annotations

import json

import pandas as pd
from UliPlot.XLSX import auto_adjust_xlsx_column_width
from utils._logging import setup_logger

logger = setup_logger()


def write_json(filepath: str, json_object: dict) -> None:
    with open(filepath, 'w') as f:
        json.dump(json_object, f, indent=4)


def write_xlsx(xlsx_file: str, dict_value: dict) -> None:
    with pd.ExcelWriter(xlsx_file, engine='xlsxwriter') as writer:
        for sheetname, df in dict_value.items():
            # df.columns = df.columns.str.upper()
            df.to_excel(writer, sheet_name=sheetname, freeze_panes=(1, 9), index=False)
            auto_adjust_xlsx_column_width(df, writer, sheet_name=sheetname, index=False)

            workbook = writer.book
            cell_format = workbook.add_format()
            cell_format.set_bold()
            cell_format.set_font_color('blue')

            header_format = workbook.add_format({'bold': True,
                                                'text_wrap': True,
                                                'valign': 'top',
                                                'align': 'middle',
                                                'fg_color': '#FFC588',  # orange
                                                'border': 1,
                                                })

            # Write the column headers with the defined format.
            ws = writer.sheets[sheetname]
            for col_num, value in enumerate(df.columns.values):
                ws.write(0, col_num, value, header_format)
            format1 = workbook.add_format({'num_format': '#,##0'})
            ws.set_column(2, 2, None, format1)
    logger.info(f'{xlsx_file=}')


if __name__ == '__main__':
    pass
