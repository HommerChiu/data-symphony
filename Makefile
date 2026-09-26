# data-symphony 常用指令。第一次使用：make up && make install && make pipeline

PY      ?= .venv/bin/python
DBT     ?= .venv/bin/dbt
# 事件來源：event-maestro 的 checkout 與它的輸出目錄
EVENT_MAESTRO_DIR ?= ../event-maestro
EVENTS_DIR        ?= $(EVENT_MAESTRO_DIR)/data
DAYS    ?= 14
USERS   ?= 300
SEED    ?= 42
DUPLICATE_RATE ?= 0.01   # event-maestro 故意寫入的重複比例，staging 會去重

export DBT_PROFILES_DIR := $(CURDIR)/dbt

.PHONY: up down clean install generate upload load dbt pipeline ingest trino

up:            ## 啟動 RustFS、Polaris、Trino（第一次會建立 bucket 與 catalog）
	docker compose up -d --wait trino

down:          ## 停止服務（資料保留在 volume）
	docker compose down

clean:         ## 停止服務並刪除所有資料
	docker compose down -v

install:       ## 建立 .venv 並安裝 pyiceberg、dbt-trino
	python3 -m venv .venv
	$(PY) -m pip install -r ingestion/requirements.txt

generate:      ## 用 event-maestro 的產生器產生歷史事件（寫到 $(EVENTS_DIR)）
	cd $(EVENT_MAESTRO_DIR) && python3 -m event_maestro -o $(abspath $(EVENTS_DIR)) generate --users $(USERS) --days $(DAYS) --seed $(SEED) --duplicate-rate $(DUPLICATE_RATE)

upload:        ## 把 $(EVENTS_DIR) 的事件檔上傳到 s3://landing/ga4_events/
	$(PY) ingestion/upload_landing.py $(EVENTS_DIR)

load:          ## landing -> Iceberg raw.ga4_events
	$(PY) ingestion/load_raw.py

dbt:           ## raw -> staging -> marts，並跑測試
	$(DBT) build --project-dir dbt

pipeline: generate upload load dbt   ## 端到端跑一次

ingest: upload load dbt   ## 不產生新資料，只把 event-maestro 目前的輸出（含網站真人操作）跑一遍

trino:         ## 開 Trino CLI
	docker compose exec trino trino --catalog iceberg
