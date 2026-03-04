ARG DEBIAN_FRONTEND=noninteractive
FROM postgres:17.2

RUN apt update -y && \
    apt install -y postgresql-plpython3-17

# Clean up
RUN apt-get -y clean all && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /tmp/* /var/tmp/*

COPY init_db.sh /docker-entrypoint-initdb.d/

EXPOSE 5432
