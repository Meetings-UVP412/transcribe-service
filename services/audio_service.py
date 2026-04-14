import requests
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class AudioService:
    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'python-audio-processor/1.0',
        })

    def get_audio_chunk(self, uuid: str, ord_num: int) -> Optional[Tuple[bytes, str]]:
        url = f"{self.api_base_url}/internal/{uuid}/chunks/{ord_num}"
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
                logger.warning(f"Чанк не найден: uuid={uuid}, ord={ord_num}")
            else:
                logger.error(f"Ошибка API ({response.status_code}): {response.text[:200]}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка сети: {str(e)}")
            return None

    def update_meeting_text(self, uuid: str, text: str) -> bool:
        url = f"{self.api_base_url}/internal/updateText/{uuid}"
        try:
            logger.info(f"Отправка текста в API: uuid={uuid}, длина={len(text)} символов")
            response = self.session.patch(
                url,
                data=text.encode('utf-8'),
                headers={'Content-Type': 'text/plain; charset=utf-8'},
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"Текст успешно добавлен к встрече {uuid}")
            return True
        except requests.exceptions.HTTPError as e:
            logger.error(f"Ошибка API при обновлении текста ({response.status_code}): {response.text[:200]}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка сети при обновлении текста: {str(e)}")
            return False
