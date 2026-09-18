.PHONY: sync lint test test-api test-index test-spark test-iceberg verify verify-ci

sync:
	uv sync --frozen --all-extras

lint:
	uv run --frozen --all-extras ruff check src tests validate_stage.py
	uv run --frozen --all-extras ruff format --check src tests validate_stage.py

test:
	uv run --frozen --all-extras pytest -m "not spark and not integration"

test-api:
	uv run --frozen --extra api pytest tests/test_api.py tests/test_api_auth.py

test-index:
	uv run --frozen --extra index pytest \
		tests/test_search_index.py tests/test_search_projection.py

test-spark:
	env -u SPARK_HOME uv run --frozen --all-extras pytest \
		-m "spark and not integration"

test-iceberg:
	RUN_ICEBERG_INTEGRATION=1 env -u SPARK_HOME \
		uv run --frozen --all-extras pytest \
		-m "spark and integration"

verify: lint test test-spark

verify-ci: verify
