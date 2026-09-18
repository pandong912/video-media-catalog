"""SparkApplication entry point bundled in the runtime image."""

from video_media_catalog.spark_cli import main


if __name__ == "__main__":
    raise SystemExit(main())
