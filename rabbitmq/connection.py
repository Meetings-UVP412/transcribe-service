import pika
from config import Config


def create_connection():
    credentials = pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASSWORD)
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=Config.RABBITMQ_HOST,
            port=Config.RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
    )


def declare_queue(channel):
    EXCHANGE_NAME = 'meetings-exchange'
    ROUTING_KEY = 'chunk.downloaded'
    QUEUE_NAME = 'chunk_downloaded_queue'

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)
    return QUEUE_NAME
