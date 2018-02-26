#!/usr/bin/env python
import pika
import uuid

class FibonacciRpcClient(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key='rpc_queue',
                                   properties=pika.BasicProperties(
                                         reply_to = self.callback_queue,
                                         correlation_id = self.corr_id,
                                         ),
                                   body=str(n))
        while self.response is None:
            self.connection.process_data_events()
        return int(self.response)

import pika

def get_fibs(n):
    fibs = [0 ,1]
    if n % 2 != 0:
        for i in range(2,n, 2):
            fibs.append(i)
    else:
        for i in range(3,n, 2):
            fibs.append(i)
    return fibs


def chunks(lst,n):
    return [lst[i::n] for i in xrange(n)]

def do(n):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    q=channel.queue_declare(queue="rpc_queue",passive=True)
    workers = q.method.consumer_count
    print('[%s] workers available' % workers)
    fibs_to_calc = get_fibs(n)
    for fib in fibs_to_calc:
        channel.basic_publish(exchange='', routing_key='rpc_queue', body=str(fib))

    result = []

    channel.queue_declare(queue='result')
    from functools import partial
    def my_callback_with_extended_args(ch, method, properties, body, result, n):
        result.append(int(body))
        if len(result) >= n:
            print sum(result)
            channel.stop_consuming()
            connection.close()

    channel.basic_consume( lambda ch, method, properties, body: my_callback_with_extended_args(ch, method, properties, body, result=result, n=len(fibs_to_calc)),
                          queue='result',
                          no_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()
