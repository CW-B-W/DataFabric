import argparse
import json
import requests
import time

def send_task(task_info: dict) -> str:
    resp = requests.post('http://192.168.103.48:5000/data_integration', data=json.dumps(task_info))
    task_id = resp.text
    return task_id

prev_status = {}
def check_task_status(task_id: str) -> bool:
    # TASKSTATUS_ACCEPTED   = 1
    # TASKSTATUS_PROCESSING = 2
    # TASKSTATUS_SUCCEEDED  = 3
    # TASKSTATUS_FAILED     = 4
    # TASKSTATUS_ERROR      = 5
    # TASKSTATUS_UNKNOWN    = 6
    resp = requests.get(f'http://192.168.103.48:5000/data_integration/status?task_id={task_id}')
    try:
        task_status = json.loads(resp.text)
        if task_id in prev_status:
            if task_status != prev_status[task_id]:
                print(task_status)
        else:
            print(task_status)
        prev_status[task_id] = task_status
    except Exception as e:
        if task_id not in prev_status:
            print(f"{task_id} doesn't exist now.")
            prev_status[task_id] = {}
        return False
    if task_status['status'] > 2:
        return True
    return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("task", type=str)
    parser.add_argument("-n", "--n_tasks", help="Number of tasks", type=int, default=1)
    args = parser.parse_args()

    filepath = args.task
    n_tasks  = args.n_tasks
    
    with open(filepath, 'r') as rf:
        task_info = json.load(rf)
    
    task_id_list  = [None for i in range(n_tasks)]
    task_req_time = [None for i in range(n_tasks)]
    task_end_time = [None for i in range(n_tasks)]
    task_status   = [False for i in range(n_tasks)]
    for i in range(n_tasks):
        task_info['task_id'] = f'StressTest_{i}'
        task_id_list[i]  = send_task(task_info)
        task_req_time[i] = time.time()

    while (False in task_status):
        for i in range(n_tasks):
            if task_status[i] == True:
                continue
            task_id = task_id_list[i]
            status = check_task_status(task_id)
            if status == True:
                task_status[i]   = True
                task_end_time[i] = time.time()
                print(f'Task {task_id} finished!')
        time.sleep(0.2)
    
    print("All Finished!")
    for i in range(n_tasks):
        print(f'Elapsed time of {task_id_list[i]}: {task_end_time[i] - task_req_time[i]}')

if __name__ == "__main__":
    main()