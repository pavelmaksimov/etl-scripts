import argparse
import asyncio
import logging
import pathlib
from datetime import timedelta, date, datetime

from marilyn_api.client import AsyncClient

from project.marilyn import helper
from project.utils.clickhouse import Client
from project.utils.dt_helper import iter_range_datetime
from project.utils.transform_funcs import to_float, to_date

PROJECT_PLACEMENTS_TABLE_NAME = "mary_placements"
STATS_TABLE_NAME = "mary_stats"

LOGGING_FORMAT = "%(asctime)s %(pathname)s:%(funcName)s %(message)s"
LOGGING_FILEPATH = pathlib.Path.cwd() / "etl-mary-clickhouse.log"

logging.basicConfig(
    format=LOGGING_FORMAT, level=logging.INFO, filename=LOGGING_FILEPATH, filemode="a"
)
logger = logging.getLogger(__name__)

logger.info("LOGGING_FILEPATH: %s", LOGGING_FILEPATH)


async def etl_project_placements(
    mary_client: AsyncClient,
    db_client: Client,
    project_id: int,
):
    logger.info("Create table %s", PROJECT_PLACEMENTS_TABLE_NAME)
    db_client.execute(
        helper.create_table_of_mary_placements_query.format(
            PROJECT_PLACEMENTS_TABLE_NAME
        )
    )

    logger.info("Export placements")
    async for page in mary_client.iter_project_placements(project_id):
        for i, item in enumerate(page["items"]):
            # Transform data types.
            page["items"][i]["placement_id"] = page["items"][i].pop("id")
            page["items"][i]["placement_name"] = page["items"][i].pop("name")
            page["items"][i]["outer_synced_at"] = to_date(
                page["items"][i]["outer_synced_at"]
            )

        logger.info("Insert data to %s", PROJECT_PLACEMENTS_TABLE_NAME)
        db_client.insert(PROJECT_PLACEMENTS_TABLE_NAME, page["items"])


async def etl_stats(
    mary_client: AsyncClient,
    db_client: Client,
    project_id,
    start_date: date,
    end_date: date,
):
    body = {
        "channel_id": [],
        "start_date": str(start_date.strftime("%Y-%m-%d")),
        "end_date": str(end_date.strftime("%Y-%m-%d")),
        "date_grouping": "day",
        "grouping": "placement",
        "filtering": [{"entity": "project", "entities": [project_id]}],
        "custom_metrics": [],
        "profiles": [],
        "goals": [],
        "with_vat": False,
        "per_page": 200,
        "sorting": "date",
        "columns": [
            "date",
            "placement_id",
            "placement_name",
            "campaign_xid",
            "channel_id",
            "impressions",
            "clicks",
            "cpm_fact",
            "ctr",
            "cost_fact",
            "cpc_fact",
            "orders",
            "model_orders",
            "revenue",
            "revenue_model_orders",
        ],
    }

    logger.info("Create table %s", STATS_TABLE_NAME)
    db_client.execute(helper.create_table_of_mary_stats_query.format(STATS_TABLE_NAME))

    logger.info("Drop partition in %s", STATS_TABLE_NAME)
    dates = iter_range_datetime(start_date, end_date, timedelta(days=1))
    db_client.drop_partitions_by_dates(STATS_TABLE_NAME, dates)

    logger.info("Export stats")
    async for page in mary_client.iter_statistics_detailed(body):
        for i, item in enumerate(page["items"]):
            # Transform data types.
            page["items"][i]["cost_fact"] = to_float(page["items"][i]["cost_fact"])
            page["items"][i]["cpc_fact"] = to_float(page["items"][i]["cpc_fact"])
            page["items"][i]["cpm_fact"] = to_float(page["items"][i]["cpm_fact"])
            page["items"][i]["ctr"] = to_float(page["items"][i]["ctr"])
            page["items"][i]["revenue"] = to_float(page["items"][i]["revenue"])
            page["items"][i]["revenue_model_orders"] = to_float(
                page["items"][i]["revenue_model_orders"]
            )
            page["items"][i]["date"] = to_date(page["items"][i]["date"])

        logger.info("Insert data to %s", STATS_TABLE_NAME)
        db_client.insert(STATS_TABLE_NAME, page["items"])


async def main(
    api_root: str,
    token: str,
    account_id: int,
    project_id: int,
    start_date: date,
    end_date: date,
    db_host: str,
    db_port: int,
    db_name: str,
    db_user: str,
    db_password: str,
):
    db_client = Client(host=db_host, port=db_port, user=db_user, password=db_password)
    headers = {
        "X-API-Account": account_id,
        "X-API-Token": token,
    }
    mary_client = AsyncClient(api_root, headers)

    # Create database.
    logger.info("Create database %s", db_name)
    db_client.execute(helper.create_database_query.format(db_name))

    # Set default database name.
    db_client.connection.database = db_name

    await etl_project_placements(mary_client, db_client, project_id)
    await etl_stats(mary_client, db_client, project_id, start_date, end_date)

    logger.info("Success")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL from Marilyn to Clickhouse")
    parser.add_argument(
        "-s",
        "--start-date",
        required=True,
        help="Start date - format YYYY-MM-DD",
        type=lambda d: datetime.strptime(d, "%Y-%m-%d"),
    )
    parser.add_argument(
        "-e",
        "--end-date",
        required=True,
        help="End date - format YYYY-MM-DD",
        type=lambda d: datetime.strptime(d, "%Y-%m-%d"),
    )
    parser.add_argument(
        "--marilyn-api-root",
        required=True,
        type=str,
        help="Api root. Example https://app.mymarilyn.ru",
    )
    parser.add_argument(
        "--marilyn-token", required=True, type=str, help="Token for auth"
    )
    parser.add_argument("--marilyn-account", required=True, type=str, help="Account ID")
    parser.add_argument("--marilyn-project", required=True, type=str, help="Project ID")
    parser.add_argument("--db-host", required=True, type=str, help="Clickhouse host")
    parser.add_argument("--db-port", required=True, type=int, help="Clickhouse port")
    parser.add_argument(
        "--db-name", required=True, type=str, help="Clickhouse database name"
    )
    parser.add_argument("--db-user", required=True, type=str, help="Clickhouse user")
    parser.add_argument(
        "--db-password", required=True, type=str, help="Clickhouse password"
    )
    args = parser.parse_args()

    asyncio.run(
        main(
            api_root=args.marilyn_api_root,
            token=args.marilyn_token,
            account_id=args.marilyn_account,
            project_id=args.marilyn_project,
            start_date=args.start_date,
            end_date=args.end_date,
            db_host=args.db_host,
            db_port=args.db_port,
            db_name=args.db_name,
            db_user=args.db_user,
            db_password=args.db_password,
        )
    )
