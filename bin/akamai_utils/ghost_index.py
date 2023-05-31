from __future__ import annotations

import numpy
from utils import _logging as lg

logger = lg.setup_logger()


def build_ghost_log_index(file_location: str):
    with open(file_location) as f:
        columns = [line.rstrip() for line in f]
    keys = [i for i, _ in enumerate(columns)]
    columns_dict = dict(zip(keys, columns))
    logger.debug(columns_dict)
    return columns, columns_dict


def log_format_url(record: str, col_number: int):

    if isinstance(col_number, numpy.float64):
        col_number = col_number.astype(numpy.int64)
    return f'http://lp.engr.akamai.com/log-format.xml#{record}{col_number}'


if __name__ == '__main__':
    pass
