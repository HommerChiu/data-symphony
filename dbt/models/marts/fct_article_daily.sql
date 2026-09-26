{#- gold：每篇文章每天的閱讀漏斗：瀏覽 -> 讀到 25/50/75/100% -> 按讚 / 收藏 / 分享 / 留言 -#}

with events as (

    select * from {{ ref('stg_ga4__events') }}
    where article_id is not null

),

daily as (

    select
        event_date,
        article_id,
        max_by(article_title, event_at) as article_title,
        max(article_topic) as article_topic,
        max(article_author) as article_author,
        max(article_word_count) as article_word_count,

        count_if(event_name = 'page_view') as views,
        count(distinct user_pseudo_id) filter (where event_name = 'page_view') as readers,
        count_if(event_name = 'select_content') as clicks_from_lists,
        -- 閱讀進度用「幾個 session 讀到」來算，避免同一個人重整頁面重複計算
        count(distinct session_key) filter (where event_name = 'read_progress' and percent_read >= 25) as sessions_read_25,
        count(distinct session_key) filter (where event_name = 'read_progress' and percent_read >= 50) as sessions_read_50,
        count(distinct session_key) filter (where event_name = 'read_progress' and percent_read >= 75) as sessions_read_75,
        count(distinct session_key) filter (where event_name = 'read_progress' and percent_read = 100) as sessions_read_100,
        avg(seconds_on_page) filter (where event_name = 'read_progress' and percent_read = 100) as avg_seconds_to_finish,
        count_if(event_name = 'like_article') as likes,
        count_if(event_name = 'bookmark_article') as bookmarks,
        count_if(event_name = 'share') as shares,
        count_if(event_name = 'submit_comment') as comments,
        count_if(event_name = 'copy_code') as code_copies
    from events
    group by event_date, article_id

)

select
    *,
    cast(sessions_read_100 as double) / nullif(views, 0) as completion_rate,
    cast(likes + bookmarks + shares + comments as double) / nullif(views, 0) as action_rate
from daily
