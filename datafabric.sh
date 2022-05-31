#!/bin/bash

function start_docker_compose() {
    docker network create -d bridge datafabric_network --ipam-driver default --subnet=172.22.0.0/16 || true
    docker-compose up -d
}

function stop_docker_compose() {
    docker-compose stop -t 300
    docker-compose down
    docker network rm datafabric_network
}

function build_dockers() {
    nohup docker-compose build > $(pwd)/docker-build.log 2>&1 &
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

function flask_cli() {
    docker exec -it datafabric-flask /bin/bash
}

function mysql_cli() {
    docker exec -it datafabric-mysql mysql --password=my-secret-pw
}

function mongo_cli() {
    docker exec -it datafabric-mongo mongosh --username=root --password=example
}

function generate_test_data() {
    docker exec -it datafabric-flask python3 /test/generate_testdata.py -t $1 -c $2
}

function restart_flask() {
    docker-compose stop -t 300 flask
    docker-compose up -d flask
}

function print_help() {
    echo -e "Usage example:"
    echo -e "\t./datafabric.sh help"
    echo -e "\t./datafabric.sh start"
    echo -e "\t./datafabric.sh stop"
    echo -e "\t./datafabric.sh restart"
    echo -e "\t./datafabric.sh build"
    echo -e "\t./datafabric.sh build-log"
    echo -e "\t./datafabric.sh bash datafabric-flask"
    echo -e "\t./datafabric.sh logs datafabric-flask"
    echo -e "\t./datafabric.sh mysql"
    echo -e "\t./datafabric.sh mongo"
    echo -e "\t./datafabric.sh generate_testdata 1000 50"
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
elif [[ "$1" == "flask-cli" ]]; then
    flask_cli
elif [[ "$1" == "generate_testdata" ]]; then
    while true; do
        read -p "This operation will overwrite all the tables in MySQL! [Y/n]" yn
        case $yn in
            [Yy]* ) generate_test_data ${2:-1000} ${3:-50}; break;;
            [Nn]* ) exit;;
            * ) echo "Please answer yes or no.";;
        esac
    done
elif [[ "$1" == "restart-flask" ]]; then
    restart_flask
else
    print_help
fi