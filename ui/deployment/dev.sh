#!/bin/bash

# Find the git root directory dynamically
GIT_ROOT=$(git rev-parse --show-toplevel)

# Check if --build parameter is passed
if [[ $1 == "--build" ]]; then
    # Run docker-compose up with project directory, verbose mode and build
    docker-compose --verbose --project-directory "$GIT_ROOT/ui" -f "$GIT_ROOT/ui/deployment/docker-compose.yml" up --build
else
    # Run docker-compose up with project directory and verbose mode
    docker-compose --verbose --project-directory "$GIT_ROOT/ui" -f "$GIT_ROOT/ui/deployment/docker-compose.yml" up
fi
