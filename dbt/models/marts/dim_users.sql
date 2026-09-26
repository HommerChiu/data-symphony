{#- gold：一列一個使用者（user_pseudo_id），含首次來源與累積行為 -#}

with sessions as (

    select * from {{ ref('fct_sessions') }}

)

select
    user_pseudo_id,
    max(user_id) as user_id,
    min(session_start_at) as first_seen_at,
    max(session_end_at) as last_seen_at,
    min_by(source, session_start_at) as first_source,
    min_by(medium, session_start_at) as first_medium,
    min_by(device_category, session_start_at) as device_category,
    min_by(country, session_start_at) as country,
    count(*) as sessions,
    sum(page_views) as page_views,
    sum(articles_completed) as articles_completed,
    sum(bookmarks) as bookmarks,
    sum(engagement_time_sec) as engagement_time_sec,
    max(user_id) is not null as is_registered,
    min(session_start_at) filter (where has_sign_up) as signed_up_at
from sessions
group by user_pseudo_id
