# Render web service (render.yaml): nginx mirrors the recorded demo that CI publishes to GitHub Pages, so a new
# demo needs no Render redeploy. The full stack is docker-compose.yml.
FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY deploy/render/nginx.render.conf /etc/nginx/conf.d/default.conf
EXPOSE 10000
