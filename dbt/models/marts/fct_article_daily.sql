{#- gold：每篇文章每天的閱讀漏斗：瀏覽 -> 讀到 90% -> 讀完 -> 收藏 / 分享 -#}

with events as (

    select * from {{ ref('stg_ga4__events') }}
    where article_id is not null

),

daily as (

    select
        event_date,
        article_id,
        max(article_category) as article_category,
        max(article_author) as article_author,
        max_by(page_title, event_at) filter (where event_name = 'page_view') as article_title,

        count_if(event_name = 'page_view') as views,
        count(distinct user_pseudo_id) filter (where event_name = 'page_view') as readers,
        count_if(event_name = 'select_content') as clicks,
        count_if(event_name = 'scroll') as scrolled_90,
        count_if(event_name = 'article_complete') as completes,
        count_if(event_name = 'bookmark') as bookmarks,
        count_if(event_name = 'share') as shares,
        avg(reading_time_sec) as avg_complete_reading_sec
    from events
    group by event_date, article_id

)

select
    *,
    cast(scrolled_90 as double) / nullif(views, 0) as scroll_rate,
    cast(completes as double) / nullif(views, 0) as completion_rate,
    cast(bookmarks + shares as double) / nullif(views, 0) as action_rate
from daily
