#!/bin/bash

if [[ -z $(which docker-compose) ]];
then
    function docker-compose() {
        docker compose --compatibility $@
    }
fi
