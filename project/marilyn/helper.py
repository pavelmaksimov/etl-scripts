create_database_query = "CREATE DATABASE IF NOT EXISTS {} ENGINE = Atomic"

create_table_of_mary_placements_query = """CREATE TABLE IF NOT EXISTS {} (
    channel_id UInt8,
    connection_id UInt32,
    project_id UInt32,
    customer_xid LowCardinality(String),
    campaign_xid LowCardinality(String),
    placement_id UInt32,
    placement_name LowCardinality(String),
    placement_type LowCardinality(String),
    status LowCardinality(String),
    labels Array(LowCardinality(String)),
    utm_campaign String,
    utm_medium String,
    utm_source String,
    outer_synced_at DateTime64
) ENGINE ReplacingMergeTree()
    ORDER BY placement_id"""

create_table_of_mary_stats_query = """CREATE TABLE IF NOT EXISTS {} (
    channel_id UInt8,
    campaign_xid String,
    placement_id UInt32,
    placement_name LowCardinality(String),
    date Date,
    impressions UInt64,
    clicks UInt64,
    cost_fact Float32,
    cpc_fact Float32,
    cpm_fact Float32,
    ctr Float32,
    model_orders UInt64,
    orders UInt64,
    revenue Float32,
    revenue_model_orders Float32
) ENGINE MergeTree()
    ORDER BY (date, channel_id, campaign_xid, placement_id, placement_name)
    PARTITION BY date"""
