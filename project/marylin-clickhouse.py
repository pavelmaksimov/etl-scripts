import argparse
import asyncio
import logging
import pathlib
from datetime import timedelta, date, datetime
from logging.handlers import RotatingFileHandler

from marilyn_api.client import AsyncClient

from marilyn import helper
from utils.clickhouse import Client
from utils.dt_helper import iter_range_datetime
from utils.transform_funcs import to_date, delete_row_on_error, to_decimal

PROJECT_PLACEMENTS_TABLE_NAME = "mary_placements"
STATS_TABLE_NAME = "mary_stats"
AD_STATS_TABLE_NAME = "mary_ad_stats"

LOGGING_FORMAT = "%(asctime)s [%(levelname)s] %(pathname)s:%(funcName)s  %(message)s"
LOGGING_FILEPATH = pathlib.Path.cwd() / "etl-mary-clickhouse.log"
LOGGING_INFO = logging.INFO

logging.basicConfig(format=LOGGING_FORMAT, level=LOGGING_INFO)
logger = logging.getLogger(__name__)
logger.info("LOGGING_FILEPATH: '%s'", LOGGING_FILEPATH)
logger.addHandler(
    RotatingFileHandler(
        LOGGING_FILEPATH, mode='a', backupCount=2, maxBytes=1024 * 1024 * 50, # 50MB
    )
)


async def etl_project_placements(
    mary_client: AsyncClient,
    db_client: Client,
    project_id: int,
    db_name: str,
):
    db_table = f"{db_name}.{PROJECT_PLACEMENTS_TABLE_NAME}"

    logger.info("Create table '%s'", db_table)
    db_client.execute(
        helper.create_table_of_mary_placements_query.format(db_table)
    )

    logger.info("Export placements")
    async for page in mary_client.iter_project_placements(project_id):
        number_of_not_inserted_rows = 0

        for i, item in enumerate(page["items"]):
            count_page_rows = len(page["items"])

            # Transform data types.
            with delete_row_on_error(page["items"], i, logger) as row:
                row["placement_id"] = row.pop("id")
                row["placement_name"] = row.pop("name")
                row["outer_synced_at"] = to_date(row["outer_synced_at"])

            number_of_not_inserted_rows += count_page_rows - len(page["items"])

        logger.info("Insert data to '%s'", db_table)
        db_client.insert(db_table, page["items"])
        logger.info("Inserted %s lines", len(page["items"]))
        logger.warning("Number of not inserted rows %s lines", number_of_not_inserted_rows)


async def etl_stats(
    mary_client: AsyncClient,
    db_client: Client,
    project_id: int,
    db_name: str,
    start_date: date,
    end_date: date,
):
    db_table = f"{db_name}.{STATS_TABLE_NAME}"

    body = {
        "channel_id": [],
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
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

    logger.info("Create table '%s'", db_table)
    db_client.execute(helper.create_table_of_mary_stats_query.format(db_table))

    logger.info("Drop partition in '%s'", db_table)
    dates = iter_range_datetime(start_date, end_date, timedelta(days=1))
    db_client.drop_partitions_by_dates(db_table, dates)

    logger.info("Export stats")
    async for page in mary_client.iter_statistics_detailed(body):
        number_of_not_inserted_rows = 0
        for i, item in enumerate(page["items"]):
            count_page_rows = len(page["items"])

            # Transform data types.
            with delete_row_on_error(page["items"], i, logger) as row:
                row["cost_fact"] = to_decimal(row["cost_fact"])
                row["cpc_fact"] = to_decimal(row["cpc_fact"])
                row["cpm_fact"] = to_decimal(row["cpm_fact"])
                row["ctr"] = to_decimal(row["ctr"])
                row["revenue"] = to_decimal(row["revenue"])
                row["revenue_model_orders"] = to_decimal(row["revenue_model_orders"])
                row["date"] = to_date(row["date"])

            number_of_not_inserted_rows += count_page_rows - len(page["items"])

        logger.info("Insert data to '%s'", db_table)
        db_client.insert(db_table, page["items"])
        logger.info("Inserted %s lines", len(page["items"]))
        logger.warning("Number of not inserted rows %s lines", number_of_not_inserted_rows)


async def etl_ad_stats(
    mary_client: AsyncClient,
    db_client: Client,
    project_id: int,
    db_name: str,
    start_date: date,
    end_date: date,
):
    db_table = f"{db_name}.{AD_STATS_TABLE_NAME}"

    body = {
        "channel_id": [],
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "date_grouping": "day",
        "grouping": "ad",
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
            "campaign_xid",
            "placement_name",
            "adgroup_id",
            "adgroup_name",
            "ad_id",
            "ad_name",
            "ad_head",
            "ad_head2",
            "ad_head3",
            "ad_text",
            "ad_text2",
            "ad_url",
            "ad_format",
            "impressions",
            "clicks",
            "video_views_start",
            "video_views_2_sec",
            "video_views_3_sec",
            "video_views_6_sec",
            "video_views",
            "video_view_rate",
            "engagements",
            "engagement_rate",
            "ctr",
            "cost",
            "cpc",
            "cpm",
            "cpv",
            "cpe",
            "cost_fact",
            "cpc_fact",
            "cpm_fact",
            "cpv_fact",
            "cpe_fact",
            "link_clicks",
            "group_clicks",
            "post_likes",
            "post_shares",
            "follows",
            "comments",
            "join_rate",
            "hides",
            "reports",
            "unsubscribes",
            "leads",
            "reach",
            "reach_total",
            "reach_total_sum",
            "viral_reach_total_sum",
            "reach_subscribers",
            "revenue",
            "revenue_model_orders",
            "orders",
            "model_orders",
            "channel_id",
        ],
    }

    logger.info("Create table '%s'", db_table)
    db_client.execute(helper.create_table_of_mary_ad_stats_query.format(db_table))

    logger.info("Drop partition in '%s'", db_table)
    dates = iter_range_datetime(start_date, end_date, timedelta(days=1))
    db_client.drop_partitions_by_dates(db_table, dates)

    logger.info("Export ad stats")
    async for page in mary_client.iter_statistics_detailed(body):
        number_of_not_inserted_rows = 0
        for i, item in enumerate(page["items"]):
            count_page_rows = len(page["items"])

            # Transform data types.
            with delete_row_on_error(page["items"], i, logger) as row:
                row["cost_fact"] = to_decimal(row["cost_fact"])
                row["cpc_fact"] = to_decimal(row["cpc_fact"])
                row["cpm_fact"] = to_decimal(row["cpm_fact"])
                row["cpv_fact"] = to_decimal(row["cpv_fact"])
                row["cpe_fact"] = to_decimal(row["cpe_fact"])
                row["cost"] = to_decimal(row["cost"])
                row["cpc"] = to_decimal(row["cpc"])
                row["ctr"] = to_decimal(row["ctr"])
                row["cpm"] = to_decimal(row["cpm"])
                row["cpe"] = to_decimal(row["cpe"])
                row["cpv"] = to_decimal(row["cpv"])
                row["revenue"] = to_decimal(row["revenue"])
                row["revenue_model_orders"] = to_decimal(row["revenue_model_orders"])
                row["video_view_rate"] = to_decimal(row["video_view_rate"])
                row["engagement_rate"] = to_decimal(row["engagement_rate"])
                row["date"] = to_date(row["date"])

            number_of_not_inserted_rows += count_page_rows - len(page["items"])

        logger.info("Insert data to '%s'", db_table)
        db_client.insert(db_table, page["items"])
        logger.info("Inserted %s lines", len(page["items"]))
        logger.warning("Number of not inserted rows %s lines", number_of_not_inserted_rows)


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
    logger.info("Create database '%s'", db_name)
    db_client.execute(helper.create_database_query.format(db_name))

    await etl_project_placements(mary_client, db_client, project_id, db_name)
    await etl_stats(mary_client, db_client, project_id, db_name, start_date, end_date)
    await etl_ad_stats(mary_client, db_client, project_id, db_name, start_date, end_date)

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
