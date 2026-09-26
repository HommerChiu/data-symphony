"""把 landing 的 GA4 JSONL 載入 Iceberg bronze 表 `raw.ga4_events`。

- schema 比照 GA4 BigQuery export（也就是 event-maestro 的 schema/ga4_event.schema.json），巢狀欄位保留成 struct / list。
  `app_info`、`event_dimensions`、`ecommerce`、`items` 在知識網站永遠是空的，所以不載入。
- 表以 event_date（字串 YYYYMMDD，跟 GA4 一樣）分區
- 每個 event_date 用 overwrite 整個分區，所以同一天重跑不會重複（idempotent）。
  注意：landing 裡「故意重複」的列（event-maestro 的 --duplicate-rate）會原樣保留，去重在 dbt staging 做。

用法：
    python ingestion/load_raw.py                        # 載入 landing 裡所有日期
    python ingestion/load_raw.py --date 2026-09-01      # 只載入某一天
"""
import argparse
import datetime as dt
import json
import warnings

import pyarrow as pa
from pyarrow import fs as pafs
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table.name_mapping import MappedField, NameMapping
from pyiceberg.transforms import IdentityTransform

from config import LANDING_PREFIX, iceberg_catalog, landing_fs

# 第一次載入某天時 overwrite 沒東西可刪，pyiceberg 會發警告，不影響結果
warnings.filterwarnings("ignore", message="Delete operation did not match any records")

TABLE = "raw.ga4_events"

VALUE_FIELDS = [
    ("string_value", pa.string()),
    ("int_value", pa.int64()),
    ("float_value", pa.float64()),
    ("double_value", pa.float64()),
]


def struct(*names, type_=pa.string()):
    return pa.struct([(n, type_) for n in names])


SCHEMA = pa.schema(
    [
        ("event_date", pa.string()),
        ("event_timestamp", pa.int64()),
        ("event_name", pa.string()),
        ("event_params", pa.list_(pa.struct([("key", pa.string()), ("value", pa.struct(VALUE_FIELDS))]))),
        ("event_previous_timestamp", pa.int64()),
        ("event_value_in_usd", pa.float64()),
        ("event_bundle_sequence_id", pa.int64()),
        ("event_server_timestamp_offset", pa.int64()),
        ("user_id", pa.string()),
        ("user_pseudo_id", pa.string()),
        ("privacy_info", struct("analytics_storage", "ads_storage", "uses_transient_token")),
        (
            "user_properties",
            pa.list_(
                pa.struct(
                    [
                        ("key", pa.string()),
                        ("value", pa.struct(VALUE_FIELDS + [("set_timestamp_micros", pa.int64())])),
                    ]
                )
            ),
        ),
        ("user_first_touch_timestamp", pa.int64()),
        ("user_ltv", pa.struct([("revenue", pa.float64()), ("currency", pa.string())])),
        (
            "device",
            pa.struct(
                [
                    ("category", pa.string()),
                    ("mobile_brand_name", pa.string()),
                    ("mobile_model_name", pa.string()),
                    ("mobile_marketing_name", pa.string()),
                    ("mobile_os_hardware_model", pa.string()),
                    ("operating_system", pa.string()),
                    ("operating_system_version", pa.string()),
                    ("vendor_id", pa.string()),
                    ("advertising_id", pa.string()),
                    ("language", pa.string()),
                    ("is_limited_ad_tracking", pa.string()),
                    ("time_zone_offset_seconds", pa.int64()),
                    ("browser", pa.string()),
                    ("browser_version", pa.string()),
                    ("web_info", struct("browser", "browser_version", "hostname")),
                ]
            ),
        ),
        ("geo", struct("city", "country", "continent", "region", "sub_continent", "metro")),
        ("traffic_source", struct("name", "medium", "source")),
        ("stream_id", pa.string()),
        ("platform", pa.string()),
        (
            "collected_traffic_source",
            struct(
                "manual_campaign_id",
                "manual_campaign_name",
                "manual_source",
                "manual_medium",
                "manual_term",
                "manual_content",
                "gclid",
                "dclid",
                "srsltid",
            ),
        ),
        ("is_active_user", pa.bool_()),
        ("batch_event_index", pa.int64()),
        # 追溯用：這筆資料從哪個檔案、什麼時候載入
        ("_source_file", pa.string()),
        ("_loaded_at", pa.timestamp("us", tz="UTC")),
    ]
)


def iceberg_schema():
    """pyarrow schema 沒有 field id，這裡依序給 id 再轉成 Iceberg schema。"""
    counter = iter(range(1, 10_000))

    def mapped(field):
        t = field.type
        children = []
        if pa.types.is_struct(t):
            children = [mapped(t.field(i)) for i in range(t.num_fields)]
        elif pa.types.is_list(t):
            children = [mapped(t.value_field.with_name("element"))]
        return MappedField(field_id=next(counter), names=[field.name], fields=children)

    mapping = NameMapping([mapped(f) for f in SCHEMA])
    return pyarrow_to_schema(SCHEMA, name_mapping=mapping)


def get_or_create_table(catalog):
    catalog.create_namespace_if_not_exists("raw")
    try:
        return catalog.load_table(TABLE)
    except NoSuchTableError:
        schema = iceberg_schema()
        spec = PartitionSpec(
            PartitionField(
                source_id=schema.find_field("event_date").field_id,
                field_id=1000,
                transform=IdentityTransform(),
                name="event_date",
            )
        )
        print(f"creating table {TABLE}")
        return catalog.create_table(TABLE, schema=schema, partition_spec=spec)


def list_dates(fs):
    infos = fs.get_file_info(pafs.FileSelector(LANDING_PREFIX, allow_not_found=True))
    # 目錄是 event_date=YYYY-MM-DD
    return sorted(i.base_name.split("=", 1)[1] for i in infos if i.type == pafs.FileType.Directory)


def read_date(fs, event_date):
    selector = pafs.FileSelector(f"{LANDING_PREFIX}/event_date={event_date}")
    rows = []
    loaded_at = dt.datetime.now(dt.timezone.utc)
    for info in fs.get_file_info(selector):
        if not info.path.endswith(".jsonl"):
            continue
        with fs.open_input_stream(info.path) as f:
            for line in f.read().decode().splitlines():
                if line.strip():
                    row = json.loads(line)
                    row["_source_file"] = f"s3://{info.path}"
                    row["_loaded_at"] = loaded_at
                    rows.append(row)
    # 分區 overwrite 的前提：檔案裡每一列的 event_date 都跟資料夾一致
    expected = event_date.replace("-", "")
    bad = {r["event_date"] for r in rows} - {expected}
    if bad:
        raise ValueError(f"event_date={event_date} 資料夾裡出現其他日期的事件：{sorted(bad)}")
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", help="YYYY-MM-DD，不給就載入全部")
    args = ap.parse_args()

    fs = landing_fs()
    table = get_or_create_table(iceberg_catalog())
    dates = [args.date] if args.date else list_dates(fs)
    for event_date in dates:
        data = read_date(fs, event_date)
        # 表裡的 event_date 跟 GA4 一樣是 YYYYMMDD 字串
        table.overwrite(data, overwrite_filter=EqualTo("event_date", event_date.replace("-", "")))
        print(f"{event_date}: loaded {data.num_rows} rows into {TABLE}")
    print(f"snapshot {table.current_snapshot().snapshot_id}")


if __name__ == "__main__":
    main()
