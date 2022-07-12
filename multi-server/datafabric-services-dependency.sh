#!/bin/bash

function start_docker_compose() {
    docker network create -d bridge datafabric_network --ipam-driver default --subnet=172.22.0.0/16 || true
    docker-compose --project-name datafabric up -d
}

function stop_docker_compose() {
    docker-compose --project-name datafabric stop -t 300
    docker-compose --project-name datafabric down
    docker network rm datafabric_network
}

function build_dockers() {
    nohup docker-compose --project-name datafabric build > $(pwd)/docker-build.log 2>&1 &
    chmod a+r $(pwd)/docker-build.log
    tail -f docker-build.log
}

function view_build_logs() {
    tail -f docker-build.log
}

function enter_docker_bash() {
    docker exec -it $1 /bin/bash
}

function view_docker_logs() {
    docker logs $1 -f
}

function mysql_cli() {
    docker exec -it datafabric_mysql_1 mysql --password=my-secret-pw
}

function mongo_cli() {
    docker exec -it datafabric_mongo_1 mongosh --username=root --password=example
}

function debug_bash() {
    sudo docker-compose --project-name datafabric run $1 /bin/bash
}

function print_help() {
    echo -e "Usage example:"
    echo -e "\t./datafabric.sh help"
    echo -e "\t./datafabric.sh start"
    echo -e "\t./datafabric.sh stop"
    echo -e "\t./datafabric.sh restart"
    echo -e "\t./datafabric.sh build"
    echo -e "\t./datafabric.sh build-log"
    echo -e "\t./datafabric.sh bash datafabric_flask_1"
    echo -e "\t./datafabric.sh logs datafabric_flask_1"
    echo -e "\t./datafabric.sh mysql"
    echo -e "\t./datafabric.sh mongo"
    echo -e "\t./datafabric.sh debug-bash"
}

if [[ "$1" == "help" ]]; then
    print_help
elif [[ "$1" == "start" ]]; then
    start_docker_compose
elif [[ "$1" == "stop" ]]; then
    stop_docker_compose
elif [[ "$1" == "restart" ]]; then
    stop_docker_compose
    start_docker_compose
elif [[ "$1" == "build" ]]; then
    build_dockers
elif [[ "$1" == "build-log" ]]; then
    view_build_logs
elif [[ "$1" == "bash" ]]; then
    enter_docker_bash $2
elif [[ "$1" == "logs" ]]; then
    view_docker_logs $2
elif [[ "$1" == "mysql" ]]; then
    mysql_cli
elif [[ "$1" == "mongo" ]]; then
    mongo_cli
elif [[ "$1" == "debug-bash" ]]; then
    debug_bash $2
else
    print_help
fi
