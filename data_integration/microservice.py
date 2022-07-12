import pika, sys, os
import urllib.parse
import json
import time
import threading
import socket

from DataIntegrator import DataIntegrator

def data_serving():
    print(f'[data_serving] starting')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", 5001))
    s.listen(5)
    print(f'[data_serving] listening')
    while True:
        print(f'[data_serving] accepting')
        client_socket, address = s.accept()
        filename  = client_socket.recv(16*1024*1024).decode()
        print(f'[data_serving] accepted {filename}')
        filepath = f'/data_serving/{filename}'
        if os.path.exists(filepath):
            print(f'[data_serving] sending file {filename}')
            client_socket.send(str.encode(f'ok'))
            with open(filepath, 'rb') as fp:
                client_socket.sendfile(fp)
        else:
            print(f'[data_serving] file not found')
            client_socket.send(str.encode(f'no'))
        client_socket.close()
        print(f'[data_serving] client_socket closed')
        sys.stdout.flush()
    return

def main():
    data_serving_thread = threading.Thread(target=data_serving)
    data_serving_thread.start()

    try_times = 10
    while try_times > 0:
        try:
            credentials = pika.PlainCredentials('guest', 'guest')
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.environ["RABBITMQ_HOST"], port=int(os.environ["RABBITMQ_PORT"]), credentials=credentials, heartbeat=0))
            channel = connection.channel()
            break
        except Exception as e:
            print(e)
            try_times -= 1
            if try_times >= 1:
                print("Connection failed. Retry in 3 seconds")
                time.sleep(3)

    channel.queue_declare(queue='task_req')

    def callback(ch, method, properties, body):
        body = urllib.parse.unquote(body.decode('utf-8'))
        onfailed_retry = 2
        while onfailed_retry >= 0:
            try:
                task_dict = json.loads(body)
                DataIntegrator.integrate(task_dict)
                break
            except:
                onfailed_retry -= 1
                if onfailed_retry < 0:
                    break

    channel.basic_consume(queue='task_req', on_message_callback=callback, auto_ack=True)

    print("start consuming")
    sys.stdout.flush()
    channel.start_consuming()

if __name__ == '__main__':
    main()
