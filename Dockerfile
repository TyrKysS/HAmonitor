ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip py3-aiohttp py3-websockets

WORKDIR /app

COPY run.sh /run.sh
COPY monitor.py /app/monitor.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
