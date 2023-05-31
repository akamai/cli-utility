from __future__ import annotations

import json
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd
from UliPlot.XLSX import auto_adjust_xlsx_column_width
from utils._logging import setup_logger


logger = setup_logger()


def write_json(filepath: str, json_object: dict) -> None:
    with open(filepath, 'w') as f:
        json.dump(dict(json_object), f, indent=4)
    filepath = Path(f'{filepath}').absolute()
    logger.debug(f'JSON file is saved locally at {str(filepath)}')


def write_xml(filepath: str, xml_data: str) -> None:
    with open(filepath, 'wb') as f:
        f.write(ET.tostring(xml_data))


def format_xlsx_header(df, writer, workbook, sheetname, show_index: bool | None = False) -> None:
    header_format = workbook.add_format({'bold': True,
                                        'text_wrap': True,
                                        'valign': 'top',
                                        'align': 'middle',
                                        'fg_color': '#FFC588',  # orange
                                        'border': 1,
                                    })
    workbook = writer.book
    ws = writer.sheets[sheetname]
    for col_num, value in enumerate(df.columns.values):
        if show_index:
            ws.write(0, col_num + 1, value, header_format)
        else:
            ws.write(0, col_num, value, header_format)
    format1 = workbook.add_format({'num_format': '#,##0'})
    ws.set_column(2, 2, None, format1)


def make_xlsx_hyperlink_to_another_sheet(filepath: str, url: str, cell: str) -> str:
    if url:
        location = f'{url}!{cell}'
        return f'=HYPERLINK("[{filepath}]{location}","{url}")'
    else:
        return f'{url}'


def write_xlsx(filepath: str, dict_value: dict,
               freeze_row: int | None = 1,
               freeze_column: int | None = 2,
               show_url: bool | None = True,
               show_index: bool | None = False) -> None:
    with pd.ExcelWriter(path=filepath, engine='xlsxwriter',
                        engine_kwargs={'options': {'strings_to_urls': show_url}}) as writer:
        writer.book.use_zip64()  # to allow excel to store files larger than 4GB
        MAX_XLXS_ROW = 1000000   # 1 million rows per sheet
        for sheetname, df in dict_value.items():
            if df is not None:
                if len(df.index) <= MAX_XLXS_ROW:
                    df.to_excel(writer, sheet_name=sheetname,
                                freeze_panes=(freeze_row, freeze_column),
                                index=show_index)
                    auto_adjust_xlsx_column_width(df, writer, sheet_name=sheetname, margin=0,
                                                  index=show_index)
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
                    ws.hide_gridlines()
                    for col_num, value in enumerate(df.columns.values):
                        if show_index:
                            ws.write(0, col_num + 1, value, header_format)
                        else:
                            ws.write(0, col_num, value, header_format)
                    format1 = workbook.add_format({'num_format': '#,##0'})
                    ws.set_column(2, 2, None, format1)
                else:
                    total, last_sheet = divmod(len(df.index), MAX_XLXS_ROW)
                    logger.info(f'{total=} {last_sheet=} dataset={len(df.index)}')
                    for sheet in (n + 1 for n in range(total)):
                        sheet_no = sheet
                        if sheet == 1:
                            first_row = 0
                        else:
                            first_row = ((sheet - 1) * MAX_XLXS_ROW) + 1
                        last_row = (sheet * MAX_XLXS_ROW) + 1
                        # print(f'Sheet{sheet}: from {first_row} to {last_row}')
                        df.iloc[first_row:last_row].to_excel(writer, sheet_name=f'{sheet_no}')
                        if sheet == total and last_sheet > 0:
                            first_row = last_row
                            last_row = (total * MAX_XLXS_ROW) + last_sheet
                            # print(f'Sheet{total+1}: from {first_row} to {last_row}')
                            sheet_no = total + 1
                            df.iloc[first_row:last_row].to_excel(writer, sheet_name=f'{sheet_no}')
                            # df.columns = df.columns.str.upper()
                        df.to_excel(writer, sheet_name=f'{sheetname}_{sheet_no}',
                                    freeze_panes=(freeze_row, freeze_column),
                                    index=show_index)
                        auto_adjust_xlsx_column_width(df, writer, sheet_name=f'{sheetname}_{sheet_no}',
                                                      index=show_index)

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
                        ws = writer.sheets[f'{sheetname}_{sheet_no}']
                        for col_num, value in enumerate(df.columns.values):
                            if show_index:
                                ws.write(0, col_num + 1, value, header_format)
                            else:
                                ws.write(0, col_num, value, header_format)

                        format1 = workbook.add_format({'num_format': '#,##0'})
                        ws.set_column(2, 2, None, format1)

    filepath = Path(f'{filepath}').absolute()
    logger.info(f'{filepath=}')


def prepare_excel_sheetname(original_string: str) -> str:
    r'''
    groups = stat_df.groupName.values.tolist()
    sheets = list(map(files.prepare_excel_sheetname, groups))
    # https://stackoverflow.com/questions/12250024/how-to-obtain-sheet-names-from-xls-files-without-loading-the-whole-file
    • The name that you type does not exceed 31 characters.
    • The name does not contain any of the following characters: : \ / ? * [ or ]
    • You did not leave the name blank.
    '''
    clean_string = original_string.replace(' - ', '-')
    clean_string = clean_string.strip(' ')
    if clean_string == original_string:
        logger.debug(f' {original_string:<50} {clean_string:<50} {len(clean_string)}')

    if len(clean_string) > 31:
        result = clean_string[:31]
        logger.debug(f'{original_string:<50} {clean_string:<50} {len(clean_string)} {result}')
    else:
        result = clean_string
        logger.debug(f' {original_string:<50} {clean_string:<50} {len(clean_string)}')
    return result


def remove_first_line_txt(filename: str) -> None:
    with open(filename) as f:
        data = f.read().splitlines(True)
    with open(filename, 'w') as f:
        f.writelines(data[1:])


class Node:
    # https://stackoverflow.com/questions/14048948/how-to-find-a-particular-json-value-by-key
    # https://stackoverflow.com/questions/70128070/how-to-a-key-recursively-in-a-dictionary-and-return-its-value
    # https://stackoverflow.com/questions/21028979/how-to-recursively-find-specific-key-in-nested-json
    # https://stackoverflow.com/questions/15196449/build-a-tree-in-python-through-recursion-by-taking-in-json-object
    def __init__(self, name=None, order=0, parent=0, level=0):
        self.name = name
        self.level = level
        self.order = order
        self.children = []
        self.parent = parent

    def __repr__(self):

        '''answer = '\n{parent:<10}{indent}{order} {name}{children}'.format(indent=self.level * '|      ',
                                                        parent=self.get_level()-1,
                                                        order=self.order,
                                                        name=self.name,
                                                        children='' if repr(self.children) == '[]' else repr(self.children).strip(']').strip('['))

        '''
        if self.level == 0:
            answer = '\n{order:<4}{name}{children}'.format(order=self.order,
                                                            name=self.name,
                                                            children='' if repr(self.children) == '[]' else repr(self.children).strip(']').strip('['))
        if self.level == 1:
            answer = '\n{order:<4}{name}{children}'.format(order=self.order,
                                                            name=self.name,
                                                            children='' if repr(self.children) == '[]' else repr(self.children).strip(']').strip('['))
        if self.level > 1:
            answer = '\n{indent}{level}.{order:<3}{name}{children}'.format(indent=(self.level - 1) * '|      ',
                                                                            order=self.order,
                                                                            name=self.name,
                                                                            level=self.level - 1,
                                                                            children='' if repr(self.children) == '[]' else repr(self.children).strip(']').strip('['))
        return answer

    def add_child(self, child):
        child.parent = self
        self.children.append(child)

    def get_level(self):
        level = 1
        p = self.parent
        while p:
            p = p.parent
            level += 1
        return level


def tree_builder(ruletree: dict, order=0, parent=0, level=0):
    node = Node(name=ruletree['name'], order=order, parent=parent, level=level)
    # start child rule member at 1
    for order, child in enumerate(ruletree['children'], 1):
        node.add_child(tree_builder(child, order, parent, level=level + 1))
    return node


if __name__ == '__main__':
    pass
