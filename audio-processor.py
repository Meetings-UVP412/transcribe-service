import pika
import json
import tempfile
import whisper
from datetime import datetime
from typing import Optional, Tuple
import requests
import logging
from config import Config

RABBITMQ_HOST = Config.RABBITMQ_HOST
RABBITMQ_PORT = Config.RABBITMQ_PORT
RABBITMQ_USER = Config.RABBITMQ_USER
RABBITMQ_PASSWORD = Config.RABBITMQ_PASSWORD

EXCHANGE_NAME = 'meetings-exchange'
ROUTING_KEY = 'chunk.downloaded'
QUEUE_NAME = 'chunk_downloaded_queue'
RESULT_ROUTING_KEY = 'chunk.processed'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

audio_service = None
whisper_model = None


def format_time(seconds: float) -> str:
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"({hours:02d}:{minutes:02d}:{secs:02d})"


class AudioService:
    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'python-audio-processor/1.0',
        })

    def get_audio_chunk(self, uuid: str, ord_num: int) -> Optional[Tuple[bytes, str]]:
        url = f"{self.api_base_url}/api/meetings/{uuid}/chunks/{ord_num}"
        try:
            logger.info(f"Запрос аудио: {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()

            content_type = response.headers.get('Content-Type', 'audio/wav')
            audio_data = response.content

            logger.info(f"Получено {len(audio_data)} байт (Content-Type: {content_type})")
            return audio_data, content_type

        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logger.warning(f"⚠️ Чанк не найден: uuid={uuid}, ord={ord_num}")
            else:
                logger.error(f"❌ Ошибка API ({response.status_code}): {response.text[:200]}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Ошибка сети: {str(e)}")
            return None


class ChunkDownloadedEvent:
    def __init__(self, uuid_str: str, ord_num: int, is_last: bool, duration: int):
        self.uuid = uuid_str
        self.ord = ord_num
        self.isLast = is_last
        self.duration = duration

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            uuid_str=str(data.get('uuid') or data.get('UUID') or data.get('Uuid') or ''),
            ord_num=int(data.get('ord') or data.get('Ord') or data.get('ORD') or 0),
            is_last=bool(data.get('isLast') or data.get('islast') or data.get('IsLast') or False),
            duration=int(data.get('duration') or data.get('Duration') or data.get('DURATION') or 0)
        )

    def __str__(self):
        return f"ChunkDownloadedEvent(uuid={self.uuid}, ord={self.ord}, isLast={self.isLast}, duration={self.duration})"


def process_event(event: ChunkDownloadedEvent) -> dict:
    logger.info(f"🔧 Начало обработки: {event}")

    # 1. Загрузка аудио С ПРОВЕРКОЙ
    result = audio_service.get_audio_chunk(event.uuid, event.ord)
    if result is None:
        raise RuntimeError(
            f"Не удалось загрузить аудио для встречи {event.uuid} чанк {event.ord}. "
            f"Проверьте: 1) Доступность {Config.MEETINGS_API_URL} 2) Существование чанка в Redis"
        )

    audio_data, content_type = result  # Теперь безопасно распаковываем

    if not audio_data or len(audio_data) < 100:
        raise ValueError(f"Получены пустые/некорректные аудиоданные ({len(audio_data)} байт)")

    suffix = '.wav'
    if 'mpeg' in content_type.lower():
        suffix = '.mp3'
    elif 'mp4' in content_type.lower() or 'm4a' in content_type.lower():
        suffix = '.m4a'

    try:
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=True, mode='wb') as tmp:
            tmp.write(audio_data)
            tmp.flush()

            logger.info(f"🧠 Запуск транскрипции (модель: small, язык: ru)...")
            result = whisper_model.transcribe(
                tmp.name,
                language="ru",
                fp16=False,
                beam_size=5,
                best_of=5,
                temperature=0.0
            )
    except Exception as e:
        logger.error(f"Ошибка транскрипции: {str(e)}")
        raise

    full_text = ""
    logger.info(f"\nТранскрипция встречи {event.uuid} (чанк {event.ord}):")
    logger.info("=" * 70)

    for segment in result["segments"]:
        start_fmt = format_time(segment["start"])
        text = segment['text'].strip()
        logger.info(f"{start_fmt} {text}")
        full_text += text + " "

    logger.info("=" * 70)
    full_text = full_text.strip()

    return {
        "uuid": event.uuid,
        "ord": event.ord,
        "isLast": event.isLast,
        "duration": event.duration,
        "transcription": full_text,
        "segments": [
            {
                "start": seg["start"],
                "end": seg["end"],
                "text": seg["text"].strip()
            } for seg in result["segments"]
        ],
        "success": True,
        "timestamp": datetime.now().isoformat(),
        "processor": "whisper-small-ru"
    }


def connect_to_rabbitmq():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
    )


def declare_queue(channel):
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)
    logger.info(f"✅ Очередь '{QUEUE_NAME}' привязана к exchange '{EXCHANGE_NAME}'")


def publish_result(channel, result: dict):
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)

    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=RESULT_ROUTING_KEY,
        body=json.dumps(result, ensure_ascii=False).encode('utf-8'),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Persistent message
            content_type='application/json',
            content_encoding='utf-8'
        )
    )
    logger.info(f"📤 Результат опубликован в очередь: {RESULT_ROUTING_KEY}")


def callback(ch, method, properties, body):
    logger.info(f"\n{'=' * 70}")
    logger.info(f"📨 Получено сообщение (delivery_tag={method.delivery_tag})")

    try:
        # Декодирование и парсинг
        body_str = body.decode('utf-8')
        data = json.loads(body_str)
        event = ChunkDownloadedEvent.from_dict(data)
        logger.info(f"📦 Распаршено событие: {event}")

        # Обработка с транскрипцией
        result = process_event(event)

        # Публикация результата
        publish_result(ch, result)

        # Подтверждение обработки
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"✅ Сообщение {method.delivery_tag} подтверждено")

    except UnicodeDecodeError:
        logger.warning("⚠️ Сообщение не в UTF-8 (возможно, Java-сериализация). Пропускаем.")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError as e:
        logger.error(f"❌ Ошибка парсинга JSON: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    except Exception as e:
        logger.exception(f"❌ Критическая ошибка обработки: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    global audio_service, whisper_model

    logger.info("🚀 Запуск Audio Processor Service")
    audio_service = AudioService(api_base_url=Config.MEETINGS_API_URL)

    logger.info("Загрузка модели Whisper 'small'...")
    try:
        whisper_model = whisper.load_model("small")
        logger.info("✅ Модель Whisper загружена")
    except Exception as e:
        logger.critical(f"❌ Невозможно загрузить модель Whisper: {e}")
        logger.critical("Убедитесь, что установлены: pip install openai-whisper && brew install ffmpeg")
        return

    try:
        connection = connect_to_rabbitmq()
        channel = connection.channel()
        declare_queue(channel)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

        logger.info(f"\n✅ Сервис готов к обработке сообщений")
        logger.info(f"Очередь: {QUEUE_NAME} | Routing Key: {ROUTING_KEY}")
        logger.info(f"Модель: whisper-small | Язык: ru")
        logger.info("=" * 70)

        channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("\n🛑 Сервис остановлен пользователем")
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"❌ Ошибка подключения к RabbitMQ: {e}")
    except Exception as e:
        logger.exception(f"❌ Неожиданная ошибка: {e}")
    finally:
        if 'connection' in locals() and connection.is_open:
            connection.close()
            logger.info("🔌 Соединение с RabbitMQ закрыто")


if __name__ == "__main__":
    main()