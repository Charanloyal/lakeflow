FROM python:3.14-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY api/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt \
    && useradd --uid 185 --no-create-home --shell /usr/sbin/nologin lakeflow \
    && mkdir -p /control/requests /control/acks && chown -R 185:185 /control && chmod 1777 /control/requests /control/acks
COPY libs/lakeflow_core /app/libs/lakeflow_core
COPY api/lakeflow_api /app/lakeflow_api
ENV PYTHONPATH=/app/libs:/app
USER 185
EXPOSE 8000
CMD ["uvicorn", "--factory", "lakeflow_api.main:create_app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*", "--no-server-header"]
