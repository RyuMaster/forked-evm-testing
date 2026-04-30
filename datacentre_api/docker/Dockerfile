FROM python:3.13-slim-bookworm

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
      build-essential \
      default-mysql-client-core \
      sqlite3 \
      curl && \
    rm -rf /var/lib/apt/lists/*

# Add user to run container as
RUN useradd -ms /bin/bash python-user

# Set working directory from where to run app
WORKDIR /usr/src/app

# Copy requirements first to take advantage of docker caching
COPY --chown=python-user:python-user requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of the files
COPY --chown=python-user:python-user . .

# Preconstruct the player-history SQLite database from SQL input
RUN rm -f data/player_history.sqlite && \
    sqlite3 data/player_history.sqlite <data/player_history.sql
ENV PLAYERHISTORY_SQLITE_DB_PATH="/usr/src/app/data/player_history.sqlite"

# Update perms and switch user to non-root before starting the app
RUN chown -R python-user:python-user /usr/src/app
USER python-user
CMD [ "python3", "main.py"]
#CMD [ "hypercorn", "-b", "0.0.0.0", "datacentre_api:app" ]
