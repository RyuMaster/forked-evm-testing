FROM mariadb:11
COPY init-databases.sh /docker-entrypoint-initdb.d/00-init-databases.sh
RUN chmod +x /docker-entrypoint-initdb.d/00-init-databases.sh
