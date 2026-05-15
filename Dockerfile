ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip && \
    pip3 install --no-cache-dir aiohttp websockets

WORKDIR /app

COPY run.sh /run.sh
COPY monitor.py /app/monitor.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
