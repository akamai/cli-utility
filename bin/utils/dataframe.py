from __future__ import annotations

import numpy as np
import pandas as pd
from utils import _logging as lg

logger = lg.setup_logger()


def explode(df, column_1: str, column_2: str, new_column: str) -> pd.DataFrame:
    vals = df[column_2].values.tolist()
    rs = [len(r) for r in vals]
    a = np.repeat(df[column_1].values, rs)
    return pd.DataFrame(np.column_stack((a, np.concatenate(vals))), columns=[column_1, new_column])


def split_rows(row, column_name: str):
    contract_id = row[column_name]
    if isinstance(contract_id, list):
        rows = []
        for id in contract_id:
            new_row = row.copy()
            new_row[column_name] = id
            rows.append(new_row)
        return pd.DataFrame(rows)
    else:
        return pd.DataFrame([row])


def split_elements_newline(elements):
    if isinstance(elements, (list, tuple)):
        return '\n'.join(map(str, elements))
    else:
        return ''


def split_elements_newline_withcomma(elements):
    return ',\n'.join(elements)


def extract_keys(dicts):
    keys = set()
    for d in dicts:
        if isinstance(d, dict):
            keys.update(d.keys())
    return keys


def explode_cell(df, column_name: str, columns_to_explode: list):
    exploded_data = []
    for _, row in df.iterrows():
        conditions = row[column_name]
        if isinstance(conditions, list) and len(conditions) > 0:
            for i in range(len(conditions)):
                new_row = row.copy()
                condition = conditions[i]
                for column in columns_to_explode:
                    if isinstance(row[column], list):
                        new_row[column] = condition.get(column, None)
                    else:
                        new_row[column] = row[column]
                exploded_data.append(new_row)
        else:
            new_row = row.copy()
            for column in columns_to_explode:
                new_row[column] = None
            exploded_data.append(new_row)
    return exploded_data


def explode_columns(row):
    exploded_row = []
    for col_name, col_value in row.items():
        if isinstance(col_value, list):
            for value in col_value:
                exploded_row.append(value)
        else:
            exploded_row.append(col_value)
    return exploded_row


def extract_dictionary_columns(row):
    extracted_values = {}
    for key, value in row.items():
        extracted_values[key] = value

    return pd.Series(extracted_values)


def json_extract(obj, search_key: str):
    """ Recursively fetch values from nested JSON. """
    values = []

    def extract(obj, values, search_key):
        """ Recursively search for values of key in JSON tree. """
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == search_key:
                    if isinstance(v, list):
                        values.extend(v)
                    else:
                        values.append(v)
                elif isinstance(v, (dict, list)):
                    extract(v, values, search_key)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, values, search_key)

    extract(obj, values, search_key)
    return values
