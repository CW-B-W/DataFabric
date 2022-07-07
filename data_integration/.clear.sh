#!/bin/bash
if [ ! -z "$1" ] && [ "$1" = "all" ]; then
    rm -f data_serving/*
fi
rm -f integration_results/*
rm -f task_logs/*
rm -f task_requests/*
