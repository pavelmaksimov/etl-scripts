from copy import deepcopy
from typing import Union, Iterator
from datetime import date, datetime, timedelta


DateOrDateTimeT = Union[date, datetime]


def iter_range_datetime(
    start_time: DateOrDateTimeT, end_time: DateOrDateTimeT, timedelta: timedelta
) -> Iterator[DateOrDateTimeT]:

    if end_time < start_time:
        raise Exception("end_time < start_time")

    date1 = deepcopy(start_time)
    while date1 <= end_time:
        yield date1
        date1 += timedelta
