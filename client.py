#!/usr/bin/env python
import pika

def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n-1) + fib(n-2)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='ec2-34-241-221-130.eu-west-1.compute.amazonaws.com', socket_timeout=3))

connection2 = pika.BlockingConnection(pika.ConnectionParameters(host='ec2-34-241-221-130.eu-west-1.compute.amazonaws.com', socket_timeout=3))


channel = connection.channel()
channel2 = connection2.channel()
channel.queue_declare(queue='rpc_queue')
channel2.queue_declare(queue='result')


def callback(ch, method, properties, body):
    n = int(body)
    result = fib(n)

    channel2.basic_publish(exchange='',
                          routing_key='result',
                          body=str(result))


channel.basic_consume(callback,
                      queue='rpc_queue',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
