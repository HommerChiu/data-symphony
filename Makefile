# data-symphony 常用指令。第一次使用：make up && make install && make pipeline

PY      ?= .venv/bin/python
DBT     ?= .venv/bin/dbt
START   ?= $(shell date -d '7 days ago' +%F 2>/dev/null || date -v-7d +%F)
DAYS    ?= 7
USERS   ?= 300
SEED    ?= 42

export DBT_PROFILES_DIR := $(CURDIR)/dbt

.PHONY: up down clean install generate load dbt pipeline trino

up:            ## 啟動 RustFS、Polaris、Trino（第一次會建立 bucket 與 catalog）
	docker compose up -d --wait trino

down:          ## 停止服務（資料保留在 volume）
	docker compose down

clean:         ## 停止服務並刪除所有資料
	docker compose down -v

install:       ## 建立 .venv 並安裝 pyiceberg、dbt-trino
	python3 -m venv .venv
	$(PY) -m pip install -r ingestion/requirements.txt

generate:      ## 產生假 GA4 事件到 s3://landing/ga4/
	$(PY) ingestion/generate_events.py --start $(START) --days $(DAYS) --users $(USERS) --seed $(SEED)

load:          ## landing -> Iceberg raw.ga4_events
	$(PY) ingestion/load_raw.py

dbt:           ## raw -> staging -> marts，並跑測試
	$(DBT) build --project-dir dbt

pipeline: generate load dbt   ## 端到端跑一次

trino:         ## 開 Trino CLI
	docker compose exec trino trino --catalog iceberg
