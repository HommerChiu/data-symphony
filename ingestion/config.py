"""連線設定：預設值對應 docker-compose.yml，可以用環境變數覆蓋。"""
import os

from pyarrow import fs as pafs
from pyiceberg.catalog import load_catalog

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "rustfsadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "rustfsadmin")
POLARIS_URI = os.environ.get("POLARIS_URI", "http://localhost:8181/api/catalog")
POLARIS_CREDENTIAL = os.environ.get("POLARIS_CREDENTIAL", "root:s3cr3t")
CATALOG = os.environ.get("POLARIS_CATALOG", "lakehouse")

# landing bucket 裡的路徑，layout 跟 event-maestro 的輸出一樣：event_date=YYYY-MM-DD/*.jsonl
LANDING_PREFIX = "landing/ga4_events"


def landing_fs():
    """landing bucket 用的 pyarrow S3 檔案系統。"""
    return pafs.S3FileSystem(
        endpoint_override=S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        region="us-east-1",
        force_virtual_addressing=False,
    )


def iceberg_catalog():
    return load_catalog(
        CATALOG,
        **{
            "type": "rest",
            "uri": POLARIS_URI,
            "warehouse": CATALOG,
            "credential": POLARIS_CREDENTIAL,
            "scope": "PRINCIPAL_ROLE:ALL",
            # RustFS 沒有 STS，不跟 Polaris 要臨時憑證，直接用自己的 key
            "header.X-Iceberg-Access-Delegation": "",
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )
