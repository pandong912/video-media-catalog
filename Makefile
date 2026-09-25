.PHONY: sync lint test test-gold-index test-spark verify verify-ci

sync:
	uv sync --frozen --all-extras

lint:
	uv run --frozen --all-extras ruff check src tests
	uv run --frozen --all-extras ruff format --check src tests

test:
	uv run --frozen --all-extras pytest -m "not spark and not integration"

test-gold-index:
	uv run --frozen --extra index pytest \
		tests/test_gold_index_cli.py \
		tests/test_gold_search_index.py \
		tests/test_gold_search_projection.py

test-spark:
	env -u SPARK_HOME uv run --frozen --all-extras pytest \
		-m "spark and not integration"

verify: lint test test-spark

verify-ci: verify
