# Test/benchmark runner on the compose network (`make integration-test`, `make benchmark`).
FROM python:3.14-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY api/requirements.txt /tmp/api-requirements.txt
COPY requirements-dev.txt /tmp/dev-requirements.txt
RUN pip install -r /tmp/api-requirements.txt -r /tmp/dev-requirements.txt \
    && useradd --uid 185 --create-home runner \
    && mkdir -p /control/requests /control/acks && chown -R 185:185 /control && chmod 1777 /control/requests /control/acks
USER runner
WORKDIR /repo
