# Serves the recorded demo that CI publishes to the gh-pages branch (built with basePath /lakeflow).
FROM alpine:3.20 AS site
ARG DEMO_ARCHIVE=https://github.com/Charanloyal/lakeflow/archive/refs/heads/gh-pages.tar.gz
ADD ${DEMO_ARCHIVE} /tmp/site.tar.gz
RUN mkdir -p /site/lakeflow \
    && tar -xzf /tmp/site.tar.gz -C /site/lakeflow --strip-components=1 \
    && test -f /site/lakeflow/index.html

FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY deploy/render/nginx.render.conf /etc/nginx/conf.d/default.conf
COPY --from=site /site /usr/share/nginx/html
EXPOSE 10000
