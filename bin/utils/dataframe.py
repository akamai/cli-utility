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