"""把 event-maestro 寫在本機的事件檔上傳到 landing bucket。

event-maestro 的輸出（見它的 docs/data-contract.md）：
    <output>/raw/ga4_events/event_date=YYYY-MM-DD/<source>.jsonl

原樣鏡像到：
    s3://landing/ga4_events/event_date=YYYY-MM-DD/<source>.jsonl

同名檔案直接覆蓋，所以重跑是安全的（collector / stream 的檔案會持續長大，覆蓋就是最新版）。

用法：
    python ingestion/upload_landing.py ../event-maestro/data
"""
import argparse
from pathlib import Path

from config import LANDING_PREFIX, landing_fs


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("output_dir", type=Path, help="event-maestro 的輸出根目錄（預設是它 repo 裡的 data/）")
    args = ap.parse_args()

    src = args.output_dir / "raw" / "ga4_events"
    files = sorted(src.glob("event_date=*/*.jsonl"))
    if not files:
        raise SystemExit(f"{src} 底下找不到任何 event_date=*/*.jsonl")

    fs = landing_fs()
    for path in files:
        key = f"{LANDING_PREFIX}/{path.parent.name}/{path.name}"
        with fs.open_output_stream(key) as out:
            out.write(path.read_bytes())
        print(f"{path} -> s3://{key}")
    print(f"uploaded {len(files)} files")


if __name__ == "__main__":
    main()
