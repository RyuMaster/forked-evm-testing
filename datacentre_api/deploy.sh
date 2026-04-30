#!/bin/bash

# Set variables
IMAGE_NAME="datacentre_api"
CONTAINER_NAME="datacentre_api"
PORT=6735
DEBUG_MODE=false

# Check if debug flag is passed
if [ "$1" == "--debug" ]; then
    DEBUG_MODE=true
    echo "🐛 Debug mode enabled"
fi

echo "🔄 Deploying $IMAGE_NAME..."

# Check if container is already running
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "🛑 Stopping existing container..."
    docker stop $CONTAINER_NAME
fi

# Remove container if it exists (even if it's not running)
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "🗑️ Removing old container..."
    docker rm $CONTAINER_NAME
fi

# Build the Docker image
echo "🏗️ Building Docker image..."
docker build -t $IMAGE_NAME -f docker/Dockerfile .

# Prepare the uvicorn command
UVICORN_CMD="uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1"

# Add debug flag if in debug mode
if [ "$DEBUG_MODE" = true ]; then
    UVICORN_CMD="$UVICORN_CMD --log-level debug --reload"
fi

# Run the container with uvicorn
echo "🚀 Starting new container..."
docker run -d \
    --name $CONTAINER_NAME \
    -p $PORT:8000 \
    --restart unless-stopped \
    $IMAGE_NAME \
    $UVICORN_CMD

echo "✅ Deployment complete!"
if [ "$DEBUG_MODE" = true ]; then
    echo "🐛 Running in debug mode with increased logging"
fi
echo "📡 Service available on port $PORT"
