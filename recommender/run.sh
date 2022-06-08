#!/bin/bash
set -e
./run_flask.sh &
start-notebook.sh &
wait