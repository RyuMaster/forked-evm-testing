FROM python:3-slim-bookworm

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
      curl \
      default-mysql-client-core \
      gcc \
      g++ \
      make && \
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

# Update perms and switch user to non-root before starting the app
RUN chown -R python-user:python-user /usr/src/app
USER python-user
CMD [ "python", "main.py" ]
