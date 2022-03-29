from datetime import datetime, date
from typing import List, Iterable, Union

from clickhouse_driver import Client as Client_


class Client(Client_):
    def drop_partitions(self, db_table: str, partition: str):
        return self.execute(f"ALTER TABLE {db_table} DROP PARTITION '{partition}'")

    def drop_partitions_by_dates(self, db_table: str, dates: Iterable[Union[date, datetime]]):
        for date in dates:
            self.drop_partitions(db_table, partition=date.strftime('%Y-%m-%d'))

    def insert(self, db_table: str, data):
        return self.execute(f"INSERT INTO {db_table} VALUES", data)
