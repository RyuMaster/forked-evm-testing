#!/bin/bash

# Check if script is run with sudo
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root or with sudo"
  exit 1
fi

# Configuration
CONTAINER_NAME="datacentre-updater"
IMAGE_NAME="datacentre-updater"
ENV_FILE_PATH=".env"

# Check if .env file exists
if [ ! -f "$ENV_FILE_PATH" ]; then
  echo "Error: .env file not found. Please create one based on example.env"
  exit 1
fi

# Extract SQLite path from .env file
SQLITE_PATH=$(grep SQLITE_PATH "$ENV_FILE_PATH" | cut -d= -f2)
if [ -z "$SQLITE_PATH" ]; then
  echo "Error: SQLITE_PATH not found in .env file"
  exit 1
fi

# Extract the directory part of the SQLite path
SQLITE_DIR=$(dirname "$SQLITE_PATH")
echo "SQLite directory path: $SQLITE_DIR"

echo "Stopping any existing container..."
# Check if container is running and stop it
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
  docker stop $CONTAINER_NAME
  echo "Existing container stopped."
fi

# Remove container if it exists
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
  docker rm $CONTAINER_NAME
  echo "Old container removed."
fi

echo "Building new Docker image..."
# Build the Docker image
docker build -t $IMAGE_NAME -f docker/Dockerfile .

echo "Starting new container..."
# Run the new container with read-only access to SQLite path
docker run -d \
  --name $CONTAINER_NAME \
  --restart unless-stopped \
  --env-file $ENV_FILE_PATH \
  --network host \
  -v "$SQLITE_DIR:$SQLITE_DIR:ro" \
  $IMAGE_NAME

echo "Deployment completed successfully!"
echo "Container is running in detached mode."
echo "To view logs: docker logs $CONTAINER_NAME"