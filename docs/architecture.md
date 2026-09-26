# 架構筆記：選擇與踩過的坑

這份記錄每個元件為什麼這樣選，以及把它們接起來時遇到的問題。之後可以整理進 through-virtuoso。

## 元件選擇

| 角色 | 選擇 | 為什麼 |
|---|---|---|
| 物件儲存 | RustFS 1.0 | MinIO 的官方 image 已經從 Docker Hub 下架（`minio/minio` 拉不到），RustFS 是 S3 相容、單一 binary、有 web console 的替代品 |
| Table format | Apache Iceberg | 開放格式，snapshot / time travel / schema evolution / hidden partitioning |
| Catalog | Apache Polaris 1.7 | Iceberg REST catalog 標準實作，有 RBAC；狀態存在 Postgres，重啟不會遺失 |
| 查詢引擎 | Trino 476 | 對 Iceberg 支援完整（含 `MERGE`、metadata 表），dbt-trino 成熟 |
| 載入 | pyiceberg + pyarrow | 不需要 Spark 就能寫 Iceberg，適合小量批次 |
| 轉換 | dbt-trino | incremental merge、測試、文件 |

## 分層與 idempotency

- **本機 → landing**：`upload_landing.py` 原樣鏡像 event-maestro 的檔案，同名覆蓋。
- **landing → bronze**：`load_raw.py` 以 `event_date` 為單位 `overwrite`（先刪該分區再寫入，一個 Iceberg snapshot 完成），
  同一天重跑結果一樣。
- **重複事件**：event-maestro 可以故意寫入完全相同的列（`--duplicate-rate`）。bronze 保留原樣（raw 就是 raw），
  staging 用 `row_number() over (partition by event_key)` 只留一筆。Trino 沒有 `QUALIFY`，所以包一層子查詢。
- **bronze → silver**：`stg_ga4__events` 是 incremental `merge`，每次只重算最近 `lookback_days`（預設 3）天，
  用 `event_key`（user、時間、事件名稱、參數的 md5）merge，晚到的事件會被補進來、不會重複。
- **silver → gold**：marts 資料量小，直接 `table` 全量重建。

## 踩過的坑

### 1. RustFS 沒有 STS，Polaris 無法發臨時憑證

Polaris 預設會用 AWS STS 幫每張表發「只能存取這張表路徑」的臨時憑證（credential vending）。
RustFS 沒有 STS，所以 catalog 設 `stsUnavailable: true`，各引擎改用自己的 S3 key：

- Trino：`iceberg.rest-catalog.vended-credentials-enabled=false` + `s3.aws-access-key`
- pyiceberg：預設會送 `X-Iceberg-Access-Delegation: vended-credentials` header，
  Polaris 會回 `Credential vending was requested ... but no credentials are available`。
  設 `header.X-Iceberg-Access-Delegation: ""` 關掉。

### 2. S3 endpoint 在容器內外不一樣

Polaris 會把 catalog 的 `endpoint` 放進 loadTable 回應的 config，pyiceberg 會用它**覆蓋**自己設定的 `s3.endpoint`。
如果填 `http://rustfs:9000`，在本機跑的 Python 會解析不到 `rustfs`。解法是分開設：

- `endpoint: http://localhost:9000`：回給 client（本機 Python）
- `endpointInternal: http://rustfs:9000`：Polaris 自己寫 metadata 用

Trino 在容器內，用自己設定的 `s3.endpoint=http://rustfs:9000`，不受影響。

### 3. `DROP TABLE` 被 Polaris 擋下

Trino 的 `DROP TABLE` 會帶 `purgeRequested=true`，Polaris 預設不允許，回 403：
`Unable to purge entity ... set DROP_WITH_PURGE_ENABLED`。
dbt 重建 table 時一定會 drop，所以 catalog 屬性加上 `polaris.config.drop-with-purge.enabled=true`。

### 4. Iceberg 只支援 `timestamp(6)`

`from_unixtime()` 回傳 `timestamp(3) with time zone`。第一次 `CREATE TABLE AS` 時 Trino 會自動轉型，
但 incremental 的暫存表不會，錯誤是 `Timestamp precision (3) not supported for Iceberg`。
在 staging 明確 `cast(... as timestamp(6) with time zone)`。

### 5. catalog_admin 預設沒有讀寫資料的權限

Polaris bootstrap 後的 `root` principal 有 `service_admin`，能建 catalog，但 catalog 裡的 `catalog_admin` role
沒有 `CATALOG_MANAGE_CONTENT`，也沒有被指派給 `service_admin`。`create_catalog.py` 會補上這兩步。

## GA4 資料模型重點

- **Session 不是單一欄位**：GA4 的 session 要用 `user_pseudo_id` + `ga_session_id`（在 `event_params` 裡）組合才唯一。
- **`event_params` 是 key-value 陣列**：staging 先用 `map_from_entries` 轉成 map，再用 `ga4_param()` macro 取值，比每個參數都 `unnest` 一次便宜。
- **Engaged session**：GA4 定義為互動 ≥ 10 秒、或 ≥ 2 個 page_view、或有轉換事件。event-maestro 把結果放在 `engaged_session_event` 參數，`fct_sessions.is_engaged` 取 session 內任一事件為 1。
- **閱讀進度**：`read_progress` 在 25 / 50 / 75 / 100% 各送一次，文章漏斗用「幾個 session 讀到」計算，避免重整頁面重複計數。
- **`traffic_source` vs `collected_traffic_source`**：前者是使用者**第一次**來的來源，後者是**這次** session 的 UTM，只出現在 session 開頭的事件，沒有值就當 `(direct) / (none)`。
