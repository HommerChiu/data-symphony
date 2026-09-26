"""產生「知識分享網站」的 GA4 事件（BigQuery export 格式），寫成 JSONL 放到 landing bucket。

event-maestro 網站上線前先用這支產生練習資料；之後 event-maestro 會把真實事件寫到同一個位置：

    s3://landing/ga4/event_date=YYYYMMDD/<檔名>.jsonl

同一個 seed 重跑會覆蓋同一個檔案（generated-seed<seed>.jsonl），所以是 idempotent 的。

每一行是一筆 GA4 export row，event_params 是 [{key, value: {string_value|int_value|double_value}}]。

用法：
    python ingestion/generate_events.py --start 2026-09-01 --days 7 --users 300
    python ingestion/generate_events.py --local out/        # 不上傳，只寫本機檔案
"""
import argparse
import datetime as dt
import json
import random
import uuid
from pathlib import Path

from config import landing_fs

ARTICLES = [
    # (article_id, title, category, author, word_count)
    ("a001", "Iceberg 表格格式入門", "lakehouse", "ben", 1800),
    ("a002", "Polaris catalog 是什麼", "lakehouse", "ben", 1500),
    ("a003", "dbt incremental model 三種策略", "dbt", "ben", 2400),
    ("a004", "用 Trino 查詢 Iceberg", "query-engine", "amy", 1200),
    ("a005", "GA4 事件模型與 BigQuery export", "analytics", "amy", 2000),
    ("a006", "Medallion 架構：bronze / silver / gold", "architecture", "ben", 1600),
    ("a007", "資料品質：用 Iceberg metadata 做檢查", "data-quality", "leo", 2200),
    ("a008", "Lightdash 快速上手", "bi", "leo", 1000),
    ("a009", "Parquet 為什麼快", "storage", "amy", 1400),
    ("a010", "Time travel 與 snapshot", "lakehouse", "leo", 1300),
]
SEARCH_TERMS = ["iceberg", "dbt", "polaris", "trino", "ga4", "snapshot", "incremental", "parquet", "lightdash"]
SOURCES = [
    # (source, medium, weight)
    ("google", "organic", 45),
    ("(direct)", "(none)", 25),
    ("twitter", "social", 10),
    ("facebook", "social", 8),
    ("newsletter", "email", 7),
    ("github.com", "referral", 5),
]
DEVICES = [
    # (category, os, browser, weight)
    ("desktop", "Windows", "Chrome", 35),
    ("desktop", "Macintosh", "Safari", 15),
    ("desktop", "Macintosh", "Chrome", 10),
    ("mobile", "iOS", "Safari", 22),
    ("mobile", "Android", "Chrome", 15),
    ("tablet", "iOS", "Safari", 3),
]
GEOS = [("Taiwan", "Taipei", 50), ("Taiwan", "Taichung", 15), ("Japan", "Tokyo", 12), ("United States", "San Francisco", 13), ("Hong Kong", "Hong Kong", 10)]
SITE = "https://event-maestro.example"


def weighted(rng, items):
    return rng.choices(items, weights=[i[-1] for i in items])[0]


def param(key, value):
    if isinstance(value, bool):
        value = int(value)
    if isinstance(value, int):
        return {"key": key, "value": {"int_value": value}}
    if isinstance(value, float):
        return {"key": key, "value": {"double_value": value}}
    return {"key": key, "value": {"string_value": str(value)}}


class User:
    def __init__(self, rng, first_seen):
        self.user_pseudo_id = f"{rng.randint(10**8, 10**9 - 1)}.{int(first_seen.timestamp())}"
        self.first_touch = first_seen
        self.user_id = None  # 註冊後才有
        self.device = weighted(rng, DEVICES)
        self.geo = weighted(rng, GEOS)
        self.first_source = weighted(rng, SOURCES)
        self.session_number = 0
        # 有些人是重度讀者
        self.loyalty = rng.random()


def micros(t):
    return int(t.timestamp() * 1_000_000)


def build_row(user, ts, name, params, session_id, source):
    row = {
        "event_date": ts.strftime("%Y%m%d"),
        "event_timestamp": micros(ts),
        "event_name": name,
        "event_params": [
            param("ga_session_id", session_id),
            param("ga_session_number", user.session_number),
            *(param(k, v) for k, v in params.items() if v is not None),
        ],
        "user_id": user.user_id,
        "user_pseudo_id": user.user_pseudo_id,
        "user_first_touch_timestamp": micros(user.first_touch),
        "device": {
            "category": user.device[0],
            "operating_system": user.device[1],
            "web_info": {"browser": user.device[2], "hostname": SITE.removeprefix("https://")},
        },
        "geo": {"country": user.geo[0], "city": user.geo[1]},
        "traffic_source": {"source": user.first_source[0], "medium": user.first_source[1], "name": "(organic)"},
        "collected_traffic_source": {"manual_source": source[0], "manual_medium": source[1]},
        "platform": "WEB",
        "stream_id": "1000000001",
    }
    return row


def simulate_session(rng, user, start, is_first_visit):
    """回傳一個 session 內的事件列表。"""
    user.session_number += 1
    session_id = int(start.timestamp())
    source = weighted(rng, SOURCES) if user.session_number > 1 else user.first_source
    events = []
    t = start

    def emit(name, **params):
        events.append(build_row(user, t, name, params, session_id, source))

    if is_first_visit:
        emit("first_visit", page_location=f"{SITE}/")
    emit("session_start", page_location=f"{SITE}/")

    referrer = "" if source[0] == "(direct)" else f"https://{source[0]}/"
    location, title = f"{SITE}/", "首頁"
    emit("page_view", page_location=location, page_title=title, page_referrer=referrer)

    # 跳出：看一眼首頁就離開
    if rng.random() < 0.4 * (1 - user.loyalty):
        t += dt.timedelta(seconds=rng.randint(1, 8))
        emit("user_engagement", engagement_time_msec=int((t - start).total_seconds() * 1000), page_location=location)
        return events

    n_pages = 1 + int(rng.expovariate(1 / (1.5 + 3 * user.loyalty)))
    for _ in range(n_pages):
        # 有時先搜尋再點文章
        if rng.random() < 0.25:
            term = rng.choice(SEARCH_TERMS)
            t += dt.timedelta(seconds=rng.randint(3, 20))
            emit("page_view", page_location=f"{SITE}/search?q={term}", page_title=f"搜尋：{term}", page_referrer=location)
            emit("view_search_results", search_term=term)
            location = f"{SITE}/search?q={term}"

        article = rng.choice(ARTICLES)
        article_id, a_title, category, author, words = article
        t += dt.timedelta(seconds=rng.randint(2, 30))
        emit("select_content", content_type="article", content_id=article_id)
        prev, location = location, f"{SITE}/articles/{article_id}"
        article_params = dict(article_id=article_id, article_category=category, article_author=author)
        emit("page_view", page_location=location, page_title=a_title, page_referrer=prev, **article_params)

        # 閱讀時間大約跟字數成正比（每分鐘 ~400 字），再乘上投入程度
        read_ratio = min(1.0, rng.betavariate(1.2 + 3 * user.loyalty, 2))
        engaged_ms = int(words / 400 * 60_000 * read_ratio) + rng.randint(1_000, 8_000)
        t += dt.timedelta(milliseconds=engaged_ms)
        if read_ratio > 0.75:
            emit("scroll", percent_scrolled=90, **article_params)
        if read_ratio > 0.9:
            emit("article_complete", reading_time_sec=engaged_ms // 1000, **article_params)
        if rng.random() < 0.06 + 0.1 * read_ratio:
            emit("bookmark", **article_params)
        if rng.random() < 0.02 + 0.06 * read_ratio:
            emit("share", method=rng.choice(["copy_link", "twitter", "facebook", "line"]), content_type="article", item_id=article_id)
        emit("user_engagement", engagement_time_msec=engaged_ms, page_location=location)

        if rng.random() < 0.15:
            break

    # 少數人會註冊，之後帶 user_id
    if user.user_id is None and user.session_number >= 2 and rng.random() < 0.08 + 0.2 * user.loyalty:
        t += dt.timedelta(seconds=rng.randint(10, 60))
        user.user_id = f"u_{uuid.UUID(int=rng.getrandbits(128)).hex[:12]}"
        emit("sign_up", method=rng.choice(["email", "github", "google"]))
    return events


def generate(start_date, days, n_users, seed):
    rng = random.Random(seed)
    base = dt.datetime.combine(start_date, dt.time(), tzinfo=dt.timezone(dt.timedelta(hours=8)))
    users = []
    by_date = {}
    for day in range(days):
        day_start = base + dt.timedelta(days=day)
        # 每天有新使用者，也有舊使用者回訪
        new_users = [User(rng, day_start + dt.timedelta(seconds=rng.randint(0, 86_399))) for _ in range(n_users // days + 1)]
        returning = [u for u in users if rng.random() < 0.08 + 0.4 * u.loyalty]
        users.extend(new_users)
        rows = []
        for u in new_users:
            rows += simulate_session(rng, u, u.first_touch, is_first_visit=True)
        for u in returning:
            start = day_start + dt.timedelta(seconds=rng.randint(0, 86_399))
            rows += simulate_session(rng, u, start, is_first_visit=False)
        for r in rows:
            by_date.setdefault(r["event_date"], []).append(r)
    return by_date


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=dt.date.fromisoformat, default=dt.date.today() - dt.timedelta(days=7))
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--users", type=int, default=300, help="總共的新使用者數")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--local", type=Path, help="寫到本機資料夾而不是 landing bucket")
    args = ap.parse_args()

    by_date = generate(args.start, args.days, args.users, args.seed)
    filename = f"generated-seed{args.seed}.jsonl"
    fs = None if args.local else landing_fs()
    for event_date, rows in sorted(by_date.items()):
        body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
        if args.local:
            path = args.local / f"event_date={event_date}" / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body)
        else:
            path = f"landing/ga4/event_date={event_date}/{filename}"
            with fs.open_output_stream(path) as f:
                f.write(body.encode())
        print(f"{event_date}: {len(rows):>6} events -> {path}")


if __name__ == "__main__":
    main()
