#!/bin/bash

# Find the git root directory dynamically
GIT_ROOT=$(git rev-parse --show-toplevel)

# Run docker-compose up with project directory and verbose mode
docker-compose --verbose --project-directory "$GIT_ROOT/ui" -f "$GIT_ROOT/ui/deployment/docker-compose.yml" down
