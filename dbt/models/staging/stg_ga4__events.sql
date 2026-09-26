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
  silver：一列一個事件，把常用的 event_params 攤平成欄位，並去掉完全重複的列。
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

keyed as (

    select
        *,
        map_from_entries(transform(event_params, p -> row(p.key, p.value))) as params,
        to_hex(md5(to_utf8(concat_ws('|',
            user_pseudo_id,
            cast(event_timestamp as varchar),
            event_name,
            cast(event_bundle_sequence_id as varchar),
            cast(batch_event_index as varchar),
            json_format(cast(event_params as json))
        )))) as event_key
    from source

),

deduped as (

    -- 完全相同的事件（例如 event-maestro 的 --duplicate-rate）只留一筆
    select *
    from (
        select *, row_number() over (partition by event_key order by _loaded_at desc) as _rn
        from keyed
    )
    where _rn = 1

),

renamed as (

    select
        event_key,
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
        coalesce({{ ga4_param('engaged_session_event', 'int') }}, 0) = 1 as is_engaged_session_event,

        -- 頁面
        {{ ga4_param('page_location') }} as page_location,
        url_extract_path({{ ga4_param('page_location') }}) as page_path,
        {{ ga4_param('page_title') }} as page_title,
        nullif({{ ga4_param('page_referrer') }}, '') as page_referrer,
        {{ ga4_param('content_group') }} as content_group,
        {{ ga4_param('engagement_time_msec', 'int') }} as engagement_time_msec,

        -- 文章
        coalesce(
            {{ ga4_param('article_id') }},
            {{ ga4_param('content_id') }},
            {{ ga4_param('item_id') }}
        ) as article_id,
        {{ ga4_param('article_title') }} as article_title,
        {{ ga4_param('author') }} as article_author,
        {{ ga4_param('topic') }} as article_topic,
        {{ ga4_param('word_count', 'int') }} as article_word_count,
        {{ ga4_param('item_list_name') }} as item_list_name,
        {{ ga4_param('percent_read', 'int') }} as percent_read,
        {{ ga4_param('seconds_on_page', 'int') }} as seconds_on_page,
        {{ ga4_param('percent_scrolled', 'int') }} as percent_scrolled,

        -- 搜尋 / 分享 / 其他互動
        lower(trim({{ ga4_param('search_term') }})) as search_term,
        {{ ga4_param('search_results', 'int') }} as search_results,
        {{ ga4_param('method') }} as method,
        {{ ga4_param('link_domain') }} as link_domain,
        {{ ga4_param('file_name') }} as file_name,
        {{ ga4_param('newsletter') }} as newsletter,

        -- 裝置 / 地區
        device.category as device_category,
        device.operating_system as device_os,
        device.web_info.browser as browser,
        device.language as device_language,
        geo.country as country,
        geo.city as city,

        -- 流量來源：traffic_source 是「第一次」來的來源，collected_traffic_source 是「這次」session 的 UTM
        traffic_source.source as first_user_source,
        traffic_source.medium as first_user_medium,
        traffic_source.name as first_user_campaign,
        collected_traffic_source.manual_source as session_source,
        collected_traffic_source.manual_medium as session_medium,
        collected_traffic_source.manual_campaign_name as session_campaign,

        _source_file,
        _loaded_at

    from deduped

)

select * from renamed
