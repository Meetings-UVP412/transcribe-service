import requests
import base64
from typing import Optional, Tuple
from config import Config
import logging

logger = logging.getLogger(__name__)


class AudioService:
    """Сервис для получения аудио через API meetings-main"""

    def __init__(self, api_base_url: str = None):
        self.api_base_url = api_base_url or Config.MEETINGS_API_URL
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'python-audio-processor/1.0',
        })

    def get_audio_chunk(self, uuid: str, ord_num: int) -> Optional[Tuple[bytes, str]]:
        """
        Получает аудио-чанк через API meetings-main
        """
        url = f"{self.api_base_url}/api/meetings/{uuid}/chunks/{ord_num}"

        try:
            logger.info(f"Запрос аудио: {url}")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            content_type = response.headers.get('Content-Type', 'application/octet-stream')
            audio_data = response.content

            logger.info(f"Получено {len(audio_data)} байт аудио, тип: {content_type}")
            return audio_data, content_type

        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logger.warning(f"Чанк не найден: uuid={uuid}, ord={ord_num}")
            else:
                logger.error(f"Ошибка API ({response.status_code}): {response.text}")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка сети при запросе аудио: {e}")
            return None

    def get_audio_chunk_base64(self, uuid: str, ord_num: int) -> Optional[str]:
        """
        Альтернативный вариант: получение аудио в base64 через JSON API

        Пример ответа от Java:
        {
            "uuid": "...",
            "ord": 5,
            "audioData": "UklGRnoGAABXQVZFZm10IBAAAAABAAEAwF0AAIC7AAACABAAZGF0YQoGAACBhYqFbF..."
        }
        """
        url = f"{self.api_base_url}/api/meetings/{uuid}/chunks/{ord_num}/base64"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()
            return data.get('audioData')

        except Exception as e:
            logger.error(f"❌ Ошибка получения base64 аудио: {e}")
            return None