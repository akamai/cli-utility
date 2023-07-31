from __future__ import annotations

from datetime import date
from datetime import datetime

import pytz
from dateutil.relativedelta import relativedelta


def get_start_end(interval: str, last: int, logger=None):
    end_date = date.today()
    end_datetime = datetime(year=end_date.year,
                               month=end_date.month,
                               day=end_date.day,
                               tzinfo=pytz.utc)

    if interval == 'MONTH':
        start = end_datetime + relativedelta(months=-last)
    if interval == 'WEEK':
        start = end_datetime + relativedelta(weeks=-last)
    if interval == 'DAY':
        start = end_datetime + relativedelta(days=-last)
    if interval == 'HOUR':
        start = end_datetime + relativedelta(hours=-last)

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
