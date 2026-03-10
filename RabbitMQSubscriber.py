import pika
import json
import uuid
from datetime import datetime

from audio_service import AudioService

RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'guest'
RABBITMQ_PASSWORD = 'guest'

EXCHANGE_NAME = 'meetings-exchange'
ROUTING_KEY = 'chunk.downloaded'
QUEUE_NAME = 'chunk_downloaded_queue'

audio_service = AudioService()


class ChunkDownloadedEvent:
    def __init__(self, uuid_str: uuid, ord: int, isLast: bool, duration: int):
        self.uuid = uuid_str
        self.ord = ord
        self.isLast = isLast
        self.duration = duration

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            uuid_str=data.get('uuid') or data.get('UUID') or data.get('Uuid'),
            ord=data.get('ord') or data.get('Ord') or data.get('ORD'),
            isLast=data.get('isLast') or data.get('islast') or data.get('IsLast'),
            duration=data.get('duration') or data.get('Duration') or data.get('DURATION')
        )

    def __str__(self):
        return f"ChunkDownloadedEvent(uuid={self.uuid}, ord={self.ord}, isLast={self.isLast}, duration={self.duration})"


def connect_to_rabbitmq():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )
    )
    return connection


def declare_queue(channel):
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type='topic',
        durable=True
    )

    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True
    )

    channel.queue_bind(
        exchange=EXCHANGE_NAME,
        queue=QUEUE_NAME,
        routing_key=ROUTING_KEY
    )

    print(f"[*] Queue '{QUEUE_NAME}' bound to exchange '{EXCHANGE_NAME}' with routing key '{ROUTING_KEY}'")


def process_event(event: ChunkDownloadedEvent) -> dict:
    print(f"\n[PROCESSING] {event}")

    path = event.uuid + "_chunk_" + event.ord

    result = {
        "uuid": event.uuid,
        "ord": event.ord,
        "processed": True,
        "timestamp": datetime.now().isoformat(),
        "result_data": f"Processed chunk {event.ord} of meeting {event.uuid}"
    }

    return result


def publish_result(channel, result: dict):
    RESULT_EXCHANGE = 'meetings-exchange'
    RESULT_ROUTING_KEY = 'chunk.processed'

    channel.exchange_declare(
        exchange=RESULT_EXCHANGE,
        exchange_type='topic',
        durable=True
    )

    channel.basic_publish(
        exchange=RESULT_EXCHANGE,
        routing_key=RESULT_ROUTING_KEY,
        body=json.dumps(result),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type='application/json',
            content_encoding='utf-8'
        )
    )

    print(f"[PUBLISHED] Result to {RESULT_EXCHANGE} with key {RESULT_ROUTING_KEY}")


def callback(ch, method, properties, body):
    print(f"\n{'=' * 60}")
    print(f"[RECEIVED] Message delivery tag: {method.delivery_tag}")
    print(f"Content-Type: {properties.content_type}")
    print(f"Content-Encoding: {properties.content_encoding}")
    print(f"Headers: {properties.headers}")

    try:
        try:
            body_str = body.decode('utf-8')
            print(f"\n[RAW BODY] {body_str}")

            data = json.loads(body_str)
            print(f"[PARSED] JSON data: {data}")

            event = ChunkDownloadedEvent.from_dict(data)
            print(f"[EVENT] {event}")

            result = process_event(event)

            publish_result(ch, result)

            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[ACK] Message {method.delivery_tag} acknowledged")

        except UnicodeDecodeError:
            print("[WARNING] Message is not UTF-8 encoded. Likely Java serialized object.")
            print(f"[HEX DUMP] {body.hex()}")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to parse JSON: {e}")
            print(f"[BODY] {body}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    except Exception as e:
        print(f"[ERROR] Exception during processing: {e}")
        import traceback
        traceback.print_exc()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    print("[STARTING] Python RabbitMQ Subscriber")

    try:
        connection = connect_to_rabbitmq()
        channel = connection.channel()

        declare_queue(channel)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=callback
        )

        print(f"\n[*] Waiting for messages. To exit press CTRL+C")
        print("=" * 60)

        channel.start_consuming()

    except KeyboardInterrupt:
        print("\n[STOPPING] Subscriber stopped by user")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"[ERROR] Connection error: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            if 'connection' in locals() and connection.is_open:
                connection.close()
                print("[CLOSED] Connection closed")
        except:
            pass


if __name__ == "__main__":
    main()
