import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    MEETINGS_API_URL = os.getenv('MEETINGS_API_URL', 'http://localhost:8100')
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
