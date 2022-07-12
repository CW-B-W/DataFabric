import pika
import time
import json
import os

class RabbitMQ:
    def __init__(self, queue):
        self.__queue = queue
        try_times = 10
        while try_times > 0:
            try:
                credentials = pika.PlainCredentials('guest', 'guest')
                self.__connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.environ["RABBITMQ_HOST"], port=int(os.environ["RABBITMQ_PORT"]), credentials=credentials, heartbeat=0))
                self.__channel = self.__connection.channel()
                self.__channel.confirm_delivery()
                break
            except Exception as ex:
                try_times -= 1
                if try_times >= 1:
                    print("[RabbitMQ] Connection failed. Retry in 3 seconds")
                    time.sleep(3)
                else:
                    raise ex

        self.__channel.queue_declare(queue=self.__queue)
    
    def send_dict(self, d: dict, retry=2):
        while retry >= 0:
            try:
                self.__channel.basic_publish(
                    exchange='', routing_key=self.__queue, body=json.dumps(d), 
                    properties=pika.BasicProperties(delivery_mode = 2), mandatory=True) # make message persistent
                return True
            except Exception as e:
                pass

            retry -= 1
            time.sleep(0.1)

        return False

    def send_str(self, s: str):
        while retry >= 0:
            try:
                self.__channel.basic_publish(
                    exchange='', routing_key=self.__queue, body=s, 
                    properties=pika.BasicProperties(delivery_mode = 2), mandatory=True) # make message persistent
                return True
            except Exception as e:
                pass

            retry -= 1
            time.sleep(0.1)
            
        return False
        

    def listen(self, callback: callable):
        self.__channel.basic_consume(queue=self.__queue, on_message_callback=callback, auto_ack=True)
        self.__channel.start_consuming()