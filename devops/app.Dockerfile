FROM postgres:latest

COPY db/raw/*.sql /docker-entrypoint-initdb.d/