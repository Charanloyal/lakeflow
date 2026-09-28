# Streaming job image: official Apache Spark + pinned, checksum-verified connector jars.
FROM spark:3.5.7-scala2.12-java17-python3-ubuntu AS runtime

USER root
ARG ICEBERG_VERSION=1.9.2
ARG SPARK_VERSION=3.5.7
RUN set -eux; \
    cd /opt/spark/jars; \
    fetch() { wget -q -O "$2" "https://repo1.maven.org/maven2/$1/$2"; echo "$3  $2" | sha1sum -c -; }; \
    fetch org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION} iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar 08f897c0683e8c4e0f67eee687bf61aef1709821; \
    fetch org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION} iceberg-aws-bundle-${ICEBERG_VERSION}.jar 29aac0b8b9271dc5aad89563b5eefe130f60739f; \
    fetch org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION} spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar b21bf0ed4083462696a2eabad735f638fb7a7e6a; \
    fetch org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION} spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar 4410ead23fc3f50cabccb70e9156c41d4c247e2d; \
    fetch org/apache/kafka/kafka-clients/3.4.1 kafka-clients-3.4.1.jar a2f55251deb3d31a21903a0cfbd4d1fa6b7463ca; \
    fetch org/apache/commons/commons-pool2/2.11.1 commons-pool2-2.11.1.jar 8970fd110c965f285ed4c6e40be7630c62db6f68

COPY spark/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

COPY libs/lakeflow_core /opt/lakeflow/libs/lakeflow_core
COPY spark/lakeflow_stream /opt/lakeflow/spark/lakeflow_stream
COPY spark/jobs /opt/lakeflow/jobs
COPY spark/entrypoint.sh /opt/lakeflow/entrypoint.sh
COPY contracts /opt/lakeflow/contracts
RUN mkdir -p /checkpoints /control/requests /control/acks \
    && chown -R spark:spark /checkpoints /control \
    && chmod 1777 /control/requests /control/acks \
    && chmod 0755 /opt/lakeflow/entrypoint.sh

ENV PYTHONPATH=/opt/lakeflow/libs:/opt/lakeflow/spark \
    PYSPARK_PYTHON=python3 \
    AWS_REGION=us-east-1
USER spark
EXPOSE 4040 9108
HEALTHCHECK --interval=15s --timeout=5s --start-period=90s --retries=4 \
  CMD python3 -c "import sys,urllib.request as u; b=u.urlopen('http://127.0.0.1:9108/metrics',timeout=4).read().decode(); sys.exit(0 if 'lakeflow_stream_query_active 1.0' in b else 1)"
ENTRYPOINT ["/opt/lakeflow/entrypoint.sh"]

# PySpark tests against the exact runtime interpreter and jars (`make test`, CI).
FROM runtime AS test
USER root
RUN pip3 install --no-cache-dir pytest==8.4.2
COPY spark/tests /opt/lakeflow/spark/tests
USER spark
WORKDIR /opt/lakeflow
HEALTHCHECK NONE
ENTRYPOINT ["bash", "-c", "export PYTHONPATH=$PYTHONPATH:/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*-src.zip) && exec python3 -m pytest -q -p no:cacheprovider /opt/lakeflow/spark/tests \"$@\"", "--"]
