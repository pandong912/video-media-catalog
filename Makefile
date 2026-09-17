.PHONY: sync lint test test-spark test-iceberg verify verify-ci

sync:
	uv sync --frozen --extra spark

lint:
	uv run --frozen ruff check src tests
	uv run --frozen ruff format --check src tests

test:
	uv run --frozen pytest -m "not spark and not integration"

test-spark:
	env -u SPARK_HOME uv run --frozen --extra spark pytest \
		-m "spark and not integration"

test-iceberg:
	RUN_ICEBERG_INTEGRATION=1 env -u SPARK_HOME \
		uv run --frozen --extra spark pytest \
		-m "spark and integration"

verify: lint test test-spark

verify-ci: verify
