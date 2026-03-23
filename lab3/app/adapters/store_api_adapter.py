import json
import logging
from typing import List
import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_api_gateway import StoreGateway

class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]):
        try:
            url = f"{self.api_base_url}/processed_agent_data"

            # Варіант 1: Використовуємо json.loads(item.model_dump_json()),
            # щоб Pydantic сам перетворив дату в рядок за стандартом ISO 8601
            payload = [json.loads(item.model_dump_json()) for item in processed_agent_data_batch]

            # Тепер payload — це чистий список словників, де замість об'єктів datetime
            # стоять рядки типу "2026-03-23T12:00:00"
            response = requests.post(url, json=payload)

            if response.status_code in (200, 201):
                logging.info(f"Успішно відправлено {len(processed_agent_data_batch)} записів.")
                return True
            else:
                logging.error(f"Помилка відправки даних. Статус: {response.status_code}. Текст: {response.text}")
                return False
        except Exception as e:
            logging.error(f"Помилка підключення до Store API: {e}")
            return False