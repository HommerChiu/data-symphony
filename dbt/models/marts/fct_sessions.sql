{#- gold：一列一個 session，包含來源、瀏覽深度、閱讀行為 -#}

with events as (

    select * from {{ ref('stg_ga4__events') }}

),

sessions as (

    select
        session_key,
        min_by(user_pseudo_id, event_at) as user_pseudo_id,
        max(user_id) as user_id,
        min(session_number) as session_number,
        min(event_at) as session_start_at,
        max(event_at) as session_end_at,
        min(event_date) as session_date,

        min_by(session_source, event_at) as source,
        min_by(session_medium, event_at) as medium,
        min_by(device_category, event_at) as device_category,
        min_by(country, event_at) as country,
        min_by(page_path, event_at) filter (where event_name = 'page_view') as landing_page,

        count_if(event_name = 'page_view') as page_views,
        count(distinct article_id) filter (where event_name = 'page_view') as articles_viewed,
        count_if(event_name = 'view_search_results') as searches,
        count_if(event_name = 'scroll') as articles_scrolled_90,
        count_if(event_name = 'article_complete') as articles_completed,
        count_if(event_name = 'bookmark') as bookmarks,
        count_if(event_name = 'share') as shares,
        coalesce(sum(engagement_time_msec), 0) / 1000.0 as engagement_time_sec,
        count_if(event_name = 'first_visit') > 0 as is_new_user,
        count_if(event_name = 'sign_up') > 0 as has_sign_up

    from events
    group by session_key

)

select
    *,
    -- GA4 定義的 engaged session：互動超過 10 秒、或 2 個以上 page_view、或有轉換
    (engagement_time_sec >= 10 or page_views >= 2 or has_sign_up) as is_engaged
from sessions
