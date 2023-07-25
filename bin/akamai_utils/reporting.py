from __future__ import annotations

from datetime import date
from datetime import datetime

import pytz
from dateutil.relativedelta import relativedelta
from utils._logging import setup_logger


logger = setup_logger()


def get_start_end():
    end_date = date.today()
    end_datetime = datetime(year=end_date.year,
                               month=end_date.month,
                               day=end_date.day,
                               tzinfo=pytz.utc)
    start = end_datetime + relativedelta(days=-90)
    end = end_datetime.isoformat().replace('+00:00', 'Z')
    start = start.isoformat().replace('+00:00', 'Z')
    print()
    logger.warning(f'Report from {start} to {end}')
    return start, end


def get_execute_report_href(links):
    for link in links:
        if link['rel'] == 'execute-report':
            return link['href']
    return None


if __name__ == '__main__':
    pass
