{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_key',
    properties={'partitioning': "ARRAY['event_date']"},
    on_schema_change='append_new_columns'
  )
}}

{#-
  silver：一列一個事件，把常用的 event_params 攤平成欄位。
  增量：只重算最近 lookback_days 天的分區，用 event_key merge 進來，重跑不會重複。
-#}

with source as (

    select * from {{ source('raw', 'ga4_events') }}
    {% if is_incremental() %}
    where event_date >= (
        select format_datetime(max(event_date) - interval '{{ var("lookback_days") }}' day, 'yyyyMMdd')
        from {{ this }}
    )
    {% endif %}

),

with_params as (

    select
        *,
        map_from_entries(transform(event_params, p -> row(p.key, p.value))) as params
    from source

),

renamed as (

    select
        to_hex(md5(to_utf8(concat_ws('|',
            user_pseudo_id,
            cast(event_timestamp as varchar),
            event_name,
            json_format(cast(event_params as json))
        )))) as event_key,

        cast(date_parse(event_date, '%Y%m%d') as date) as event_date,
        -- Iceberg 只支援 timestamp(6)，from_unixtime 回傳的是 timestamp(3)
        cast(from_unixtime(event_timestamp / 1e6) at time zone '{{ var("site_timezone") }}' as timestamp(6) with time zone) as event_at,
        event_name,

        user_pseudo_id,
        user_id,
        cast(from_unixtime(user_first_touch_timestamp / 1e6) at time zone '{{ var("site_timezone") }}' as timestamp(6) with time zone) as user_first_touch_at,

        -- session：GA4 的 session 要用 user_pseudo_id + ga_session_id 才唯一
        {{ ga4_param('ga_session_id', 'int') }} as ga_session_id,
        concat(user_pseudo_id, '-', cast({{ ga4_param('ga_session_id', 'int') }} as varchar)) as session_key,
        {{ ga4_param('ga_session_number', 'int') }} as session_number,

        -- 頁面
        {{ ga4_param('page_location') }} as page_location,
        url_extract_path({{ ga4_param('page_location') }}) as page_path,
        {{ ga4_param('page_title') }} as page_title,
        nullif({{ ga4_param('page_referrer') }}, '') as page_referrer,
        {{ ga4_param('engagement_time_msec', 'int') }} as engagement_time_msec,

        -- 知識分享網站自訂參數
        coalesce(
            {{ ga4_param('article_id') }},
            {{ ga4_param('content_id') }},
            {{ ga4_param('item_id') }}
        ) as article_id,
        {{ ga4_param('article_category') }} as article_category,
        {{ ga4_param('article_author') }} as article_author,
        {{ ga4_param('percent_scrolled', 'int') }} as percent_scrolled,
        {{ ga4_param('reading_time_sec', 'int') }} as reading_time_sec,
        lower({{ ga4_param('search_term') }}) as search_term,
        {{ ga4_param('method') }} as method,

        -- 裝置 / 地區 / 流量來源
        device.category as device_category,
        device.operating_system as device_os,
        device.web_info.browser as browser,
        geo.country as country,
        geo.city as city,
        collected_traffic_source.manual_source as session_source,
        collected_traffic_source.manual_medium as session_medium,
        traffic_source.source as first_user_source,
        traffic_source.medium as first_user_medium,

        _loaded_at

    from with_params

)

select * from renamed
