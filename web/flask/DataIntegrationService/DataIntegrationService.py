from InternalMQ.RabbitMQ import RabbitMQ
from InternalDB.RedisDB import RedisDB
import threading
import json

task_req_mq    = RabbitMQ('task_req')
task_status_mq = RabbitMQ('task_status')
cache_db       = RedisDB(db=0)

def send_task(task_info: dict):
    task_req_mq.send_dict(task_info)

monitor_thread = None
def start_monitor_task_status(background: bool = True):
    global monitor_thread
    if background:
        monitor_thread = threading.Thread(target=task_status_mq.listen, args=[received_task_status])
        monitor_thread.start()
    else:
        task_status_mq.listen(received_task_status)
        

def received_task_status(ch, method, properties, body):
    task_status = json.loads(body.decode('utf-8'))
    task_id     = str(task_status['task_id'])
    cache_db.set_json(f'DataIntegrationService/task_status/{task_id}', task_status)


def get_task_status(task_id: str) -> dict:
    task_status = cache_db.get_json(f'DataIntegrationService/task_status/{task_id}')
    return task_status