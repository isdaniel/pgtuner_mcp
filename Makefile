PG ?= 16
PYTHON ?= python

.PHONY: test test-unit test-integration up down

test: test-unit

test-unit:
	pytest

test-integration:
	PGTUNER_TEST_PG_VERSION=$(PG) PGTUNER_STATEMENT_TIMEOUT_MS=2000 pytest -m integration -v

up:
	cd docker && docker compose -f docker-compose.test.yml up -d pg$(PG)

down:
	cd docker && docker compose -f docker-compose.test.yml down
