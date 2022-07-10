#!/bin/bash
if [ ! -z "$1" ] && [ "$1" = "all" ]; then
    rm -rf data_serving/ && git checkout data_serving
fi
rm -rf integration_results/ && git checkout integration_results
rm -rf task_logs/ && git checkout task_logs
rm -rf task_requests/ && git checkout task_requests
