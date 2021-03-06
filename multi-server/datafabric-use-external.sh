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

function flask_cli() {
    docker exec -it datafabric_flask_1 /bin/bash
}

function flask_logs() {
    tail -f web/flask/datafabric.log
}

function generate_test_data() {
    stop_docker_compose
    start_docker_compose

    local gen_rating=""
    if [[ "$4" == "1" ]]; then
        gen_rating="-r"
    fi
    echo "Start generating testdata..."
    docker exec -it datafabric_flask_1 python3 /test/generate_testdata.py -t $1 -c $2 -u $3 $gen_rating
    echo "Start training initial recommender..."
    train_recommender

    stop_docker_compose
    start_docker_compose
}

function restart_flask() {
    docker-compose --project-name datafabric stop -t 300 flask
    docker-compose --project-name datafabric up -d flask
}

function initialize_datafabric() {
    docker network create --driver=bridge --subnet=172.22.0.0/24 datafabric_network

    echo -e "Initializing Datafabric metadata..."
    docker-compose --project-name datafabric run --name datafabric_flask_1 --rm --no-deps flask python3 /flask-share/initialize_datafabric.py

    stop_docker_compose
}

function train_recommender() {
    docker exec -it datafabric_flask_1 curl http://datafabric_recommender_1:5000/train?implicit_pref=True
}

function debug_bash() {
    sudo docker-compose --project-name datafabric run $1 /bin/bash
}

function print_help() {
    echo -e "Usage example:"
    echo -e "\t./datafabric.sh help"
    echo -e "\t./datafabric.sh initialize"
    echo -e "\t./datafabric.sh start"
    echo -e "\t./datafabric.sh stop"
    echo -e "\t./datafabric.sh restart"
    echo -e "\t./datafabric.sh build"
    echo -e "\t./datafabric.sh build-log"
    echo -e "\t./datafabric.sh bash datafabric_flask_1"
    echo -e "\t./datafabric.sh logs datafabric_flask_1"
    echo -e "\t./datafabric.sh flask-cli"
    echo -e "\t./datafabric.sh generate_testdata 1000 200 50 1"
    echo -e "\t./datafabric.sh train_recommender"
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
elif [[ "$1" == "flask-cli" ]]; then
    flask_cli
elif [[ "$1" == "flask-logs" ]]; then
    flask_logs
elif [[ "$1" == "generate_testdata" ]]; then
    while true; do
        read -p "This operation will overwrite all the tables in MySQL! [Y/n]" yn
        case $yn in
            [Yy]* ) generate_test_data ${2:-1000} ${3:-200} ${4:-50} ${5:-1}; break;;
            [Nn]* ) exit;;
            * ) echo "Please answer yes or no.";;
        esac
    done
elif [[ "$1" == "restart-flask" ]]; then
    restart_flask
elif [[ "$1" == "initialize" ]]; then
    initialize_datafabric
elif [[ "$1" == "train_recommender" ]]; then
    train_recommender
elif [[ "$1" == "debug-bash" ]]; then
    debug_bash $2
else
    print_help
fi
