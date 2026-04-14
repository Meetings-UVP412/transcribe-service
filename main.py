from services.transcribe_service import TranscribeService
from config import Config
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def main():
    service = TranscribeService(
        rabbitmq_host=Config.RABBITMQ_HOST,
        rabbitmq_port=Config.RABBITMQ_PORT,
        api_base_url=Config.MEETINGS_API_URL
    )
    service.start()


if __name__ == "__main__":
    main()
