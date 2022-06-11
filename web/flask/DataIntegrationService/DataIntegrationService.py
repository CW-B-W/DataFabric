from InternalMQ.RabbitMQ import RabbitMQ
import threading
import json

task_req_mq    = RabbitMQ('task_req')
task_status_mq = RabbitMQ('task_status')

def send_task(task_info: dict):
    task_req_mq.send_dict(task_info)

monitor_thread = None
def start_monitor_task_status(background: bool = True):
    global monitor_thread
    if background:
        monitor_thread = threading.Thread(target=task_status_mq.listen, args=[received_task_status])
    else:
        task_status_mq.listen(received_task_status)
        

task_status_dict = {}
def received_task_status(ch, method, properties, body):
    global task_status_dict
    task_status = json.loads(body.decode('utf-8'))
    task_id     = str(task_status['task_id'])
    task_status_dict[task_id] = task_status


def get_task_status(task_id: str) -> dict:
    if task_id in task_status_dict:
        return task_status_dict[task_id]
    else:
        return None