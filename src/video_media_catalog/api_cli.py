"""Executable entry point for the catalog API."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence

from video_media_catalog.api import APISettings, create_app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-api",
        description="Serve the authenticated read-only media catalog API.",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("MEDIA_CATALOG_API_HOST", "0.0.0.0"),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("MEDIA_CATALOG_API_PORT", "8080")),
    )
    parser.add_argument(
        "--log-level",
        choices=("critical", "error", "warning", "info"),
        default=os.environ.get("MEDIA_CATALOG_API_LOG_LEVEL", "info"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    if not 1 <= parsed.port <= 65535:
        raise ValueError("API port must be between 1 and 65535")
    settings = APISettings.from_env()

    import uvicorn

    uvicorn.run(
        create_app(settings),
        host=parsed.host,
        port=parsed.port,
        log_level=parsed.log_level,
        proxy_headers=False,
        server_header=False,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
