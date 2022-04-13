from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Union, List

from dateutil import parser


@contextmanager
def delete_row_on_error(dataset: List[dict], row_index: int, logger):
    try:
        yield dataset[row_index]
    except (TypeError, ValueError, IndexError, AttributeError):
        logger.exception("Missing line due to transform error: %s", dataset[row_index])
        del dataset[row_index]


def to_decimal(value: Union[Decimal, str, int, float], quantize: Decimal = Decimal("1.00")) -> Decimal:
    return Decimal(value or 0).quantize(quantize)


def to_float(value: Union[Decimal, str, int, float]) -> float:
    return float(to_decimal(value))


def to_date(value: str) -> Union[date, datetime]:
    return parser.parse(value)
