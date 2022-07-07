import pika
import time
import json

class RabbitMQ:
    def __init__(self, queue):
        self.__queue = queue
        try_times = 10
        while try_times > 0:
            try:
                credentials = pika.PlainCredentials('guest', 'guest')
                self.__connection = pika.BlockingConnection(pika.ConnectionParameters(host='datafabric_rabbitmq_1', credentials=credentials, heartbeat=0))
                self.__channel = self.__connection.channel()
                break
            except:
                try_times -= 1
                if try_times >= 1:
                    print("Connection failed. Retry in 3 seconds")
                    time.sleep(3)

        self.__channel.queue_declare(queue=self.__queue)
    
    def send_dict(self, d: dict):
        self.__channel.basic_publish(
            exchange='', routing_key=self.__queue, body=json.dumps(d), 
            properties=pika.BasicProperties(delivery_mode = 2)) # make message persistent

    def send_str(self, s: str):
        self.__channel.basic_publish(
            exchange='', routing_key=self.__queue, body=s, 
            properties=pika.BasicProperties(delivery_mode = 2)) # make message persistent

    def listen(self, callback: callable):
        self.__channel.basic_consume(queue=self.__queue, on_message_callback=callback, auto_ack=True)
        self.__channel.start_consuming()