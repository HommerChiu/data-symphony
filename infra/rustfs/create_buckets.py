"""建立 S3 bucket（已存在就略過）。用法：python create_buckets.py landing warehouse

只用 Python 標準函式庫手刻 SigV4 簽章，init container 不必 pip install。
"""
import datetime
import hashlib
import hmac
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
REGION = os.environ.get("AWS_REGION", "us-east-1")
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def _hmac(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def signed_request(method, path):
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = now.strftime("%Y%m%d")
    host = urllib.parse.urlparse(ENDPOINT).netloc
    headers = {"host": host, "x-amz-content-sha256": EMPTY_SHA256, "x-amz-date": amz_date}
    signed_headers = ";".join(sorted(headers))
    canonical = "\n".join(
        [method, path, "", *(f"{k}:{headers[k]}" for k in sorted(headers)), "", signed_headers, EMPTY_SHA256]
    )
    scope = f"{date}/{REGION}/s3/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    key = _hmac(_hmac(_hmac(_hmac(("AWS4" + SECRET_KEY).encode(), date), REGION), "s3"), "aws4_request")
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    )
    req = urllib.request.Request(ENDPOINT + path, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


for name in sys.argv[1:]:
    if signed_request("HEAD", f"/{name}") == 200:
        print(f"bucket {name} already exists")
        continue
    status = signed_request("PUT", f"/{name}")
    assert status == 200, f"建立 bucket {name} 失敗：HTTP {status}"
    print(f"created bucket {name}")
