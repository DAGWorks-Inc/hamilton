#!/bin/bash

export DOCKER_CLI_EXPERIMENTAL=enabled

# Check if Buildx is already enabled; create a builder instance if not
docker buildx inspect hamilton-builder > /dev/null 2>&1 || \
    docker buildx create --use --name hamilton-builder

FRONTEND_IMAGE="dagworks/ui-frontend"
BACKEND_IMAGE="dagworks/ui-backend"
VERSION="latest" # can be modified to a desired version

docker buildx build --platform linux/amd64,linux/arm64 \
    -t $BACKEND_IMAGE:$VERSION -t $BACKEND_IMAGE:latest \
    --push -f backend/Dockerfile.backend backend/

docker buildx build --platform linux/amd64,linux/arm64 \
    -t $FRONTEND_IMAGE:$VERSION -t $FRONTEND_IMAGE:latest \
    --push -f frontend/Dockerfile.frontend frontend/
