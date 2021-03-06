create_database_query = "CREATE DATABASE IF NOT EXISTS {}"

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
    placement_name LowCardinality(Nullable(String)),
    date Date,
    impressions Nullable(UInt64),
    clicks Nullable(UInt64),
    cost_fact Nullable(Float32),
    cpc_fact Nullable(Float32),
    cpm_fact Nullable(Float32),
    ctr Nullable(Float32),
    model_orders Nullable(UInt64),
    orders Nullable(UInt64),
    revenue Nullable(Float32),
    revenue_model_orders Nullable(Float32)
) ENGINE MergeTree()
    ORDER BY (date, channel_id, campaign_xid, placement_id)
    PARTITION BY date"""

create_table_of_mary_ad_stats_query = """CREATE TABLE IF NOT EXISTS {} (
    channel_id UInt8,
    campaign_xid String,
    placement_id UInt32,
    placement_name LowCardinality(Nullable(String)),
    adgroup_id UInt64,
    adgroup_name LowCardinality(Nullable(String)),
    ad_id UInt64,
    ad_name Nullable(String),
    ad_head Nullable(String),
    ad_head2 Nullable(String),
    ad_head3 Nullable(String),
    ad_text Nullable(String),
    ad_text2 Nullable(String),
    ad_format LowCardinality(Nullable(String)),
    date Date,
    
    impressions Nullable(UInt64),
    clicks Nullable(UInt64),
    cost Nullable(Float32),
    cost_fact Nullable(Float32),
    cpc_fact Nullable(Float32),
    cpm_fact Nullable(Float32),
    cpv_fact Nullable(Float32),
    cpe_fact Nullable(Float32),
    ctr Nullable(Float32),
    cpc Nullable(Float32),
    cpm Nullable(Float32),
    cpv Nullable(Float32),
    cpe Nullable(Float32),
    model_orders Nullable(UInt64),
    orders Nullable(UInt64),
    revenue Nullable(Float32),
    revenue_model_orders Nullable(Float32),
    leads Nullable(UInt64),
    link_clicks Nullable(UInt64),
    group_clicks Nullable(UInt64),
    post_likes Nullable(UInt64),
    post_shares Nullable(UInt64),
    follows Nullable(UInt64),
    comments Nullable(UInt64),
    join_rate Nullable(UInt64),
    hides Nullable(UInt64),
    reports Nullable(UInt64),
    unsubscribes Nullable(UInt64),
    reach Nullable(UInt64),
    reach_total Nullable(UInt64),
    reach_total_sum Nullable(UInt64),
    reach_subscribers Nullable(UInt64),
    viral_reach_total_sum Nullable(UInt64),
    video_views_start Nullable(UInt64),
    video_views_2_sec Nullable(UInt64),
    video_views_3_sec Nullable(UInt64),
    video_views_6_sec Nullable(UInt64),
    video_views Nullable(UInt64),
    video_view_rate Nullable(Float32),
    engagements Nullable(UInt64),
    engagement_rate Nullable(Float32)
) ENGINE MergeTree()
    ORDER BY (date, channel_id, campaign_xid, placement_id, adgroup_id, ad_id)
    PARTITION BY date"""
