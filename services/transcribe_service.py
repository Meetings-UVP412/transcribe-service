import whisper
import tempfile
import logging
import pika
import json
from datetime import datetime
from models.events import ChunkDownloadedEvent
from services.audio_service import AudioService
from rabbitmq.connection import create_connection, declare_queue
from rabbitmq.publisher import publish_result

logger = logging.getLogger(__name__)


class TranscribeService:
    def __init__(self, rabbitmq_host: str, rabbitmq_port: int, api_base_url: str):
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.audio_service = AudioService(api_base_url)
        self.whisper_model = None
        self.connection = None
        self.channel = None

    def initialize(self):
        logger.info("Загрузка модели Whisper...")
        try:
            self.whisper_model = whisper.load_model("small")
            logger.info("Модель Whisper загружена")
        except Exception as e:
            logger.critical(f"Модель Whisper не загружена: {e}")
            raise

    def process_event(self, event: ChunkDownloadedEvent) -> dict:
        logger.info(f"Начало обработки: {event}")

        result = self.audio_service.get_audio_chunk(event.uuid, event.ord)
        if result is None:
            raise RuntimeError(f"Не удалось загрузить аудио для встречи: {event.uuid}, чанк: {event.ord}. ")

        audio_data, content_type = result

        if not audio_data or len(audio_data) < 100:
            raise ValueError(f"Получены пустые аудиоданные ({len(audio_data)} байт)")

        suffix = '.wav'
        if 'mpeg' in content_type.lower():
            suffix = '.mp3'
        elif 'mp4' in content_type.lower() or 'm4a' in content_type.lower():
            suffix = '.m4a'

        try:
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=True, mode='wb') as tmp:
                tmp.write(audio_data)
                tmp.flush()

                logger.info(f"Запуск транскрипции...")
                result = self.whisper_model.transcribe(
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
            text = segment['text'].strip()
            full_text += text + " "

        logger.info("=" * 70)
        full_text = full_text.strip()
        logger.warning(f"fullText: {full_text}")

        if not self.audio_service.update_meeting_text(event.uuid, full_text):
            logger.warning(f"Не удалось сохранить текст в redis для встречи {event.uuid}")

        return {
            "uuid": event.uuid,
            "ord": event.ord,
            "isLast": event.isLast,
            "duration": event.duration,
            "success": True,
            "timestamp": datetime.now().isoformat()
        }

    def callback(self, ch, method, properties, body):
        logger.info(f"\n{'=' * 70}")
        logger.info(f"Получено сообщение (delivery_tag={method.delivery_tag})")

        try:
            body_str = body.decode('utf-8')
            data = json.loads(body_str)
            event = ChunkDownloadedEvent.from_dict(data)
            logger.info(f"Распаршено событие: {event}")

            result = self.process_event(event)

            if event.isLast:
                publish_result(ch, result)

            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"Сообщение {method.delivery_tag} подтверждено")

        except UnicodeDecodeError:
            logger.warning("Сообщение не в UTF-8")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        except Exception as e:
            logger.exception(f"Критическая ошибка обработки: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def start(self):
        logger.info("Запуск Audio Processor Service")
        try:
            self.initialize()

            self.connection = create_connection()
            self.channel = self.connection.channel()

            queue_name = declare_queue(self.channel)
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback)

            logger.info(f"\nСервис готов к обработке сообщений")
            logger.info(f"Очередь: {queue_name} | Routing Key: chunk.downloaded")
            logger.info("=" * 70)

            self.channel.start_consuming()

        except KeyboardInterrupt:
            logger.info("\nСервис остановлен пользователем")
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Ошибка подключения к RabbitMQ: {e}")
        except Exception as e:
            logger.exception(f"Ошибка: {e}")
        finally:
            if self.connection and self.connection.is_open:
                self.connection.close()
                logger.info("Соединение с RabbitMQ закрыто")
