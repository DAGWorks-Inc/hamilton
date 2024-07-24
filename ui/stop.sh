#!/bin/bash

. common.sh

# Run docker-compose up with project directory and verbose mode
docker-compose --verbose -f docker-compose.yml down
