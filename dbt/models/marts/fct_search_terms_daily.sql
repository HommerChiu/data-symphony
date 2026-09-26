{#- gold：站內搜尋關鍵字，以及搜尋後有沒有點進文章 -#}

with events as (

    select * from {{ ref('stg_ga4__events') }}

),

searches as (

    select event_date, session_key, event_at, search_term
    from events
    where event_name = 'view_search_results'

),

clicks as (

    select session_key, event_at
    from events
    where event_name = 'select_content'

),

search_with_click as (

    -- 同一個 session 裡，搜尋後 5 分鐘內有點文章就算「有結果」
    select
        s.event_date,
        s.search_term,
        s.session_key,
        count(c.event_at) > 0 as has_click
    from searches s
    left join clicks c
        on c.session_key = s.session_key
        and c.event_at between s.event_at and s.event_at + interval '5' minute
    group by s.event_date, s.search_term, s.session_key, s.event_at

)

select
    event_date,
    search_term,
    count(*) as searches,
    count(distinct session_key) as sessions,
    cast(count_if(has_click) as double) / count(*) as click_through_rate
from search_with_click
group by event_date, search_term
