FROM amazoncorretto:17-al2023-headless AS java-runtime

RUN cp -a \
    "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
    /opt/java

FROM python:3.12-slim-trixie

COPY --from=java-runtime /opt/java /opt/java

ARG SPARK_VERSION=3.5.5
ARG ICEBERG_VERSION=1.8.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_JAVA_SDK_BUNDLE_VERSION=1.12.780
ARG SPARK_SHA512=ec5ff678136b1ff981e396d1f7b5dfbf399439c5cb853917e8c954723194857607494a89b7e205fce988ec48b1590b5caeae3b18e1b5db1370c0522b256ff376
ARG ICEBERG_RUNTIME_SHA512=7445f9b3962d6382f4de8040a51c62950c39649fb204373bc8fabb41ff7127224c04b4dc9c792b696589f26325f60d13c56cd5b25d9b8127972386cf8ed659dd
ARG ICEBERG_AWS_SHA512=ba928446b65c2fe030beddb827acab12db9f38e17620a29dc82041bae9b556f2266df76f40f78070192db265545fdf6e75e0b149548e52dc06c81ea98c1a1058
ARG HADOOP_AWS_SHA1=a65839fbf1869f81a1632e09f415e586922e4f80
ARG AWS_JAVA_SDK_BUNDLE_SHA1=308a3af95a47e0c4e1f8bd98a37657d4661ae45e

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    SPARK_HOME=/opt/spark \
    JAVA_HOME=/opt/java \
    PYSPARK_PYTHON=/app/.venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python \
    PATH="/app/.venv/bin:/opt/spark/bin:/opt/java/bin:$PATH" \
    PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends -o Acquire::Retries=5 \
        bash ca-certificates curl procps tini \
    && rm -rf /var/lib/apt/lists/* \
    && curl --fail --location --retry 5 --output /tmp/spark.tgz \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && echo "${SPARK_SHA512}  /tmp/spark.tgz" | sha512sum --check --strict \
    && tar --extract --gzip --directory /opt --file /tmp/spark.tgz \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "$SPARK_HOME" \
    && cp "$SPARK_HOME/kubernetes/dockerfiles/spark/entrypoint.sh" \
        /opt/entrypoint.sh \
    && chmod 0755 /opt/entrypoint.sh \
    && curl --fail --location --retry 5 --output "$SPARK_HOME/jars/iceberg-spark-runtime.jar" \
        "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    && curl --fail --location --retry 5 --output "$SPARK_HOME/jars/iceberg-aws-bundle.jar" \
        "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    && curl --fail --location --retry 5 --output "$SPARK_HOME/jars/hadoop-aws.jar" \
        "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl --fail --location --retry 5 --output "$SPARK_HOME/jars/aws-java-sdk-bundle.jar" \
        "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar" \
    && echo "${ICEBERG_RUNTIME_SHA512}  $SPARK_HOME/jars/iceberg-spark-runtime.jar" \
        | sha512sum --check --strict \
    && echo "${ICEBERG_AWS_SHA512}  $SPARK_HOME/jars/iceberg-aws-bundle.jar" \
        | sha512sum --check --strict \
    && echo "${HADOOP_AWS_SHA1}  $SPARK_HOME/jars/hadoop-aws.jar" \
        | sha1sum --check --strict \
    && echo "${AWS_JAVA_SDK_BUNDLE_SHA1}  $SPARK_HOME/jars/aws-java-sdk-bundle.jar" \
        | sha1sum --check --strict \
    && rm /tmp/spark.tgz \
    && pip install --no-cache-dir "uv==0.9.7" \
    && groupadd --system --gid 10001 catalog \
    && useradd --system --uid 10001 --gid catalog \
        --home-dir /nonexistent --shell /usr/sbin/nologin catalog

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY contracts ./contracts
COPY stage.py /opt/video-media-catalog/stage.py
RUN uv sync --frozen --no-dev \
    && mkdir --parents \
        "$SPARK_HOME/work-dir" /tmp/spark-local /tmp/spark-warehouse \
    && chown --recursive 10001:10001 \
        /app /opt/video-media-catalog "$SPARK_HOME/work-dir" \
        /tmp/spark-local /tmp/spark-warehouse

WORKDIR /opt/spark/work-dir
USER 10001:10001

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["video-media-catalog-spark", "--help"]
