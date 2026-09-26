{#- gold：站內搜尋關鍵字、結果數，以及搜尋後有沒有點進文章 -#}

with events as (

    select * from {{ ref('stg_ga4__events') }}

),

searches as (

    select event_date, session_key, event_at, search_term, search_results
    from events
    where event_name = 'view_search_results'

),

clicks as (

    select session_key, event_at
    from events
    where event_name = 'select_content' and item_list_name = 'search_results'

),

search_with_click as (

    -- 同一個 session 裡，搜尋後 5 分鐘內從搜尋結果點了文章就算「有點擊」
    select
        s.event_date,
        s.search_term,
        s.session_key,
        s.search_results,
        count(c.event_at) > 0 as has_click
    from searches s
    left join clicks c
        on c.session_key = s.session_key
        and c.event_at between s.event_at and s.event_at + interval '5' minute
    group by s.event_date, s.search_term, s.session_key, s.event_at, s.search_results

)

select
    event_date,
    search_term,
    count(*) as searches,
    count(distinct session_key) as sessions,
    avg(search_results) as avg_results,
    count_if(search_results = 0) as zero_result_searches,
    cast(count_if(has_click) as double) / count(*) as click_through_rate
from search_with_click
group by event_date, search_term
