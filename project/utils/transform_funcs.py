from datetime import date, datetime
from decimal import Decimal
from typing import Union

from dateutil import parser


def to_decimal(value: Union[Decimal, str, int, float]) -> Decimal:
    return Decimal(value or 0).quantize(Decimal("1.00"))


def to_float(value: Union[Decimal, str, int, float]) -> float:
    return float(to_decimal(value))


def to_date(value: str) -> Union[date, datetime]:
    return parser.parse(value)
