FROM apache/superset:4.1.4

USER root

RUN pip install --upgrade pip
RUN pip install clickhouse-connect
RUN pip install psycopg2-binary

COPY ./superset-config.py /app/
ENV SUPERSET_CONFIG_PATH=/app/superset-config.py

COPY ./superset-init.sh /superset-init.sh
RUN chmod +x /superset-init.sh

USER superset
ENTRYPOINT [ "/superset-init.sh" ]