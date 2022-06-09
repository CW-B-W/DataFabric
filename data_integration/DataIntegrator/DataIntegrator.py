import os
import sys
import time
import logging
from datetime import datetime

import pandas as pd
from pandasql import sqldf
from sqlalchemy import create_engine
import json

from DBMSAccessor import DBMSAccessor

def setup_logging(filename):
    save_dir = r'/task_logs/'
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - [%(levelname)s]\n%(message)s\n[%(pathname)s %(funcName)s %(lineno)d]\n',
                    filename=save_dir+filename,
                    filemode='w')
    # Until here logs only to file: 'logs_file'

    # define a new Handler to log to console as well
    console = logging.StreamHandler()
    # optional, set the logging level
    console.setLevel(logging.DEBUG)
    # set a format which is the same for console use
    # formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter('%(asctime)s - [%(levelname)s]\n%(message)s\n[%(pathname)s %(funcName)s %(lineno)d]\n')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger().addHandler(console)

    # Set all other logger levels to WARNING
    for logger_name in logging.root.manager.loggerDict:
        if logger_name != __name__:
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.WARNING)
    
    # for logger in loggers:


''' ========== RabbitMQ ========== '''
import pika, sys

TASKSTATUS_ACCEPTED   = 1
TASKSTATUS_PROCESSING = 2
TASKSTATUS_SUCCEEDED  = 3
TASKSTATUS_FAILED     = 4
TASKSTATUS_ERROR      = 5
TASKSTATUS_UNKNOWN    = 6
def send_task_status(task_id, status, message):
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='datafabric-rabbitmq', credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='task_status')

    payload = {
        'task_id': task_id,
        'status':  status,
        'message': message
    }
    body = json.dumps(payload)

    channel.basic_publish(exchange='', routing_key='task_status', body=body)
    connection.close()
''' ========== RabbitMQ ========== '''


def integrate(task_dict: dict):
    try:
        task_id = str(task_dict['task_id'])
    except Exception as e:
        print(str(e), file=sys.stderr)
        send_task_status(str(-1), TASKSTATUS_UNKNOWN, str(e))
        exit(1)

    ts = str(datetime.now().timestamp())
    setup_logging('joined_' + task_id + '_' + ts + '.log')
    logging.info('Task started. task_id = ' + task_id)
    send_task_status(task_id, TASKSTATUS_PROCESSING, '')

    logging.info('Task info:\n' + json.dumps(task_dict))

    task_list = task_dict['task_list']
    for task_idx, task_info in enumerate(task_list):
        logging.info(f"Start processing the {task_idx}-th task")
        send_task_status(task_id, TASKSTATUS_PROCESSING, f"Start processing the {task_idx}-th task")
        for i, d in enumerate(task_info['db']):
            username   = d['username']
            password   = d['password']
            ip         = d['ip']
            port       = d['port']
            dbms       = d['dbms']
            db         = d['db']
            table      = d['table']
            columns    = d['columns']
            try:
                start_time = d['start_time']
                end_time   = d['end_time']
                time_col   = d['time_column']
            except:
                start_time = None
                end_time   = None
                time_col   = None

            if dbms != 'dataframe':
                try:
                    queried_df = DBMSAccessor.query_table(
                        username, password,
                        ip, port, dbms,
                        db, table, columns,
                        start_time, end_time, time_col
                    )
                    logging.info(f"Retrieving data from {dbms}")
                    send_task_status(task_id, TASKSTATUS_PROCESSING, f"Retrieving data from {dbms}")
                    globals()[f'df{i}'] = queried_df
                except Exception as e:
                    logging.error(f"Failed to retrieve data from {dbms}" + str(e))
                    send_task_status(task_id, TASKSTATUS_FAILED, f"Failed to retrieve data from {dbms}" + str(e))
                    exit(1)
            logging.info(f'Finished retrieving table {i} from {dbms}')
            
            # make all column names uppercase
            try:
                if 'namemapping' in d:
                    namemapping = d['namemapping']
                    # make mapping key uppercase
                    namemapping =  {k.upper(): v for k, v in namemapping.items()}
                    # make original columns uppercase
                    globals()[f'df{i}'].columns = map(str.upper, globals()[f'df{i}'].columns)
                    globals()[f'df{i}'].rename(columns=namemapping, inplace=True)
                else:
                    globals()[f'df{i}'].columns = map(str.upper, globals()[f'df{i}'].columns)
                logging.debug(str(globals()[f'df{i}']))
            except Exception as e:
                logging.error("Error in renaming columns: " + str(e))
                send_task_status(task_id, TASKSTATUS_FAILED, "Error in renaming columns: " + str(e))
                exit(1)

        if len(task_info['db']) < 2:
            df_joined = df0
        else:
            # use pandasql to join tables
            try:
                pysqldf   = lambda q: sqldf(q, globals())
                logging.info('Start joining two tables')
                send_task_status(task_id, TASKSTATUS_PROCESSING, "Start joining two tables")
                df_joined = pysqldf(task_info['join_sql'])
                logging.info('Finished joining two tables')
                send_task_status(task_id, TASKSTATUS_PROCESSING, "Finished joining two tables")
            except Exception as e:
                logging.error("Error in joining the two tables: " + str(e))
                send_task_status(task_id, TASKSTATUS_FAILED, "Error in joining the two tables: " + str(e))
                exit(1)

        try:
            columns_order = task_info['result']['columns']
            df_joined = df_joined.reindex(columns_order, axis=1)
            logging.debug(str(df_joined))
        except Exception as e:
            logging.error("Error in joining the two tables. Please check if duplicated columns exist: " + str(e))
            send_task_status(task_id, TASKSTATUS_FAILED, "Error in joining the two tables. Please check if duplicated columns exist: " + str(e))
            exit(1)

        df0 = df_joined # reuse df0 if it's a pipeline task



    # ========== After all pipeline tasks are finished ==========

    if df_joined.empty:
        logging.error("The joined table is empty.")
        send_task_status(task_id, TASKSTATUS_FAILED, "The joined table is empty.")
        exit(1)

    df_joined.to_csv('/integration_results/' + task_id + '_' + ts + '.csv', index=False, header=True)

    logging.error("Job finished")
    send_task_status(task_id, TASKSTATUS_SUCCEEDED, "Job finished.")
    exit()