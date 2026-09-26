{#- gold：一列一個 session，包含來源、瀏覽深度、閱讀與互動行為 -#}

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

        -- collected_traffic_source 只出現在 session 開頭的事件，沒有 UTM 就當 direct
        coalesce(min_by(session_source, event_at) filter (where session_source is not null), '(direct)') as source,
        coalesce(min_by(session_medium, event_at) filter (where session_medium is not null), '(none)') as medium,
        min_by(session_campaign, event_at) filter (where session_campaign is not null) as campaign,
        min_by(device_category, event_at) as device_category,
        min_by(country, event_at) as country,
        min_by(page_path, event_at) filter (where event_name = 'page_view') as landing_page,

        count_if(event_name = 'page_view') as page_views,
        count(distinct article_id) filter (where event_name = 'page_view' and article_id is not null) as articles_viewed,
        count_if(event_name = 'search') as searches,
        count(distinct article_id) filter (where event_name = 'read_progress' and percent_read >= 50) as articles_read_50,
        count(distinct article_id) filter (where event_name = 'read_progress' and percent_read = 100) as articles_completed,
        count_if(event_name = 'like_article') as likes,
        count_if(event_name = 'bookmark_article') as bookmarks,
        count_if(event_name = 'share') as shares,
        count_if(event_name = 'submit_comment') as comments,
        coalesce(sum(engagement_time_msec), 0) / 1000.0 as engagement_time_sec,
        bool_or(event_name = 'first_visit') as is_new_user,
        bool_or(event_name = 'sign_up') as has_sign_up,
        bool_or(event_name = 'subscribe_newsletter') as has_newsletter_subscribe,
        -- GA4 的 engaged session：互動 >= 10 秒、>= 2 個 page_view、或有轉換；event-maestro 已經算好放在參數裡
        bool_or(is_engaged_session_event) as is_engaged

    from events
    group by session_key

)

select * from sessions
