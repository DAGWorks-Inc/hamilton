#!/bin/bash

export DOCKER_CLI_EXPERIMENTAL=enabled

# check if Docker Buildx is installed
check_buildx_installed() {
    if ! docker buildx version &> /dev/null; then
        echo "Error: Docker Buildx is not installed. Please install Docker Buildx to proceed."
        exit 1
    fi
}

# fetches the latest version of sf-hamilton-ui from PyPI
get_latest_version() {
    curl -s https://pypi.org/pypi/sf-hamilton-ui/json | \
    jq -r '.info.version'
}

check_buildx_installed

VERSION=$(get_latest_version)

echo "Using sf-hamilton-ui version: $VERSION"

# check if Buildx is already enabled; create a builder instance if not
docker buildx inspect hamilton-builder > /dev/null 2>&1 || \
    docker buildx create --use --name hamilton-builder

FRONTEND_IMAGE="dagworks/ui-frontend"
BACKEND_IMAGE="dagworks/ui-backend"

# define common platforms/architectures
PLATFORMS="linux/amd64,linux/arm64"

docker buildx build --platform $PLATFORMS \
    -t $BACKEND_IMAGE:$VERSION -t $BACKEND_IMAGE:latest \
    --push -f backend/Dockerfile.backend backend/

docker buildx build --platform $PLATFORMS \
    -t $FRONTEND_IMAGE:$VERSION -t $FRONTEND_IMAGE:latest \
    --push -f frontend/Dockerfile.frontend frontend/
