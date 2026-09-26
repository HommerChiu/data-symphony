{#- gold：一列一個匿名使用者（user_pseudo_id），含首次來源與累積行為 -#}

with sessions as (

    select * from {{ ref('fct_sessions') }}

),

first_touch as (

    -- traffic_source 是 GA4 記錄的「第一次」來源，每個事件都帶
    select
        user_pseudo_id,
        min(user_first_touch_at) as first_touch_at,
        min_by(first_user_source, event_at) as first_source,
        min_by(first_user_medium, event_at) as first_medium,
        min_by(first_user_campaign, event_at) as first_campaign
    from {{ ref('stg_ga4__events') }}
    group by user_pseudo_id

)

select
    s.user_pseudo_id,
    max(s.user_id) as user_id,
    min(f.first_touch_at) as first_touch_at,
    min(s.session_start_at) as first_seen_at,
    max(s.session_end_at) as last_seen_at,
    min(f.first_source) as first_source,
    min(f.first_medium) as first_medium,
    min(f.first_campaign) as first_campaign,
    min_by(s.device_category, s.session_start_at) as device_category,
    min_by(s.country, s.session_start_at) as country,
    count(*) as sessions,
    sum(s.page_views) as page_views,
    sum(s.articles_completed) as articles_completed,
    sum(s.likes) as likes,
    sum(s.bookmarks) as bookmarks,
    sum(s.engagement_time_sec) as engagement_time_sec,
    max(s.user_id) is not null as is_registered,
    bool_or(s.has_newsletter_subscribe) as is_newsletter_subscriber
from sessions s
left join first_touch f on f.user_pseudo_id = s.user_pseudo_id
group by s.user_pseudo_id
