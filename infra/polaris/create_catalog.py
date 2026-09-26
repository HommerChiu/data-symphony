"""在 Polaris 建立 Iceberg catalog `lakehouse`，並讓 root principal 擁有完整權限。

只用 Python 標準函式庫，方便在任何 python image 裡直接執行；重複執行是安全的。
"""
import json
import os
import urllib.error
import urllib.parse
import urllib.request

POLARIS_URL = os.environ.get("POLARIS_URL", "http://localhost:8181")
CATALOG = os.environ.get("POLARIS_CATALOG", "lakehouse")
# endpoint：回傳給 client（本機 Python）的位址；endpointInternal：Polaris 自己連線用
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_ENDPOINT_INTERNAL = os.environ.get("S3_ENDPOINT_INTERNAL", "http://rustfs:9000")


def request(method, path, body=None, token=None, form=False):
    headers = {}
    data = None
    if body is not None:
        if form:
            data = urllib.parse.urlencode(body).encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        else:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(POLARIS_URL + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def main():
    status, body = request(
        "POST",
        "/api/catalog/v1/oauth/tokens",
        {
            "grant_type": "client_credentials",
            "client_id": os.environ["POLARIS_CLIENT_ID"],
            "client_secret": os.environ["POLARIS_CLIENT_SECRET"],
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        form=True,
    )
    assert status == 200, f"取得 token 失敗：{status} {body}"
    token = body["access_token"]

    catalog = {
        "catalog": {
            "name": CATALOG,
            "type": "INTERNAL",
            "properties": {
                "default-base-location": "s3://warehouse",
                # dbt 重建 table 時會 DROP TABLE ... PURGE，預設 Polaris 不允許
                "polaris.config.drop-with-purge.enabled": "true",
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": ["s3://warehouse/"],
                "endpoint": S3_ENDPOINT,
                "endpointInternal": S3_ENDPOINT_INTERNAL,
                "pathStyleAccess": True,
                # RustFS 沒有 AWS STS，所以 Polaris 不發臨時憑證，各引擎用自己的 S3 key
                "stsUnavailable": True,
            },
        }
    }
    status, body = request("POST", "/api/management/v1/catalogs", catalog, token)
    if status == 409:
        print(f"catalog {CATALOG} already exists")
    else:
        assert status in (200, 201), f"建立 catalog 失敗：{status} {body}"
        print(f"created catalog {CATALOG}")

    # catalog_admin 預設只能管理 catalog，本身沒有讀寫資料的權限，這裡補上
    status, body = request(
        "PUT",
        f"/api/management/v1/catalogs/{CATALOG}/catalog-roles/catalog_admin/grants",
        {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
        token,
    )
    assert status in (200, 201, 204), f"授權失敗：{status} {body}"

    status, body = request(
        "PUT",
        f"/api/management/v1/principal-roles/service_admin/catalog-roles/{CATALOG}",
        {"catalogRole": {"name": "catalog_admin"}},
        token,
    )
    assert status in (200, 201, 204), f"指派 catalog role 失敗：{status} {body}"
    print("grants ok")


if __name__ == "__main__":
    main()
