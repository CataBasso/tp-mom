import pika
import random
import string
from .middleware import( 
    MessageMiddlewareQueue, 
    MessageMiddlewareExchange, 
    MessageMiddlewareMessageError, 
    MessageMiddlewareDisconnectedError, 
    MessageMiddlewareCloseError,
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self._consuming = False

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try: 
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self._consuming = True 
            self.channel.start_consuming()

            self._consuming = False
        except pika.exceptions.AMQPConnectionError as e:
            self._consuming = False
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            self._consuming = False
            raise MessageMiddlewareMessageError(str(e))
        
    def stop_consuming(self):
        if not self._consuming:
            return

        try:
            self.channel.stop_consuming()
            self._consuming = False
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            self.channel.close()
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
