FROM nginx:alpine

ARG BUILD_TIME=unknown

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY src/ /usr/share/nginx/html/

RUN sed -i "s|__BUILD_TIME__|${BUILD_TIME}|g" /usr/share/nginx/html/index.html
