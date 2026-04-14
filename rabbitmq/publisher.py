import pika
import json
import logging

EXCHANGE_NAME = 'meetings-exchange'
RESULT_ROUTING_KEY = 'chunk.processed'

logger = logging.getLogger(__name__)


def publish_result(channel, result: dict):
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)

    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=RESULT_ROUTING_KEY,
        body=json.dumps(result, ensure_ascii=False).encode('utf-8'),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type='application/json',
            content_encoding='utf-8'
        )
    )
    logger.info("Результат опубликован в очередь: {RESULT_ROUTING_KEY}")

    return f"Результат опубликован в очередь: {RESULT_ROUTING_KEY}"
