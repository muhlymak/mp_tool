import time
import pandas as pd
from loguru import logger
from helpers.telegram import send_telegram_message
from repricers.wb.api_client import WbApiClient


class CardsConnector:
    def __init__(self, from_db, to_db):
        self.from_db = from_db
        self.to_db = to_db
        self.api_client = WbApiClient()

    def _send_telegram(self, message: str):
        send_telegram_message(message)

    def _get_data(self, from_db):
        """
        Забираем данные по картчокам из БД MP
        """
        self.query = """
            WITH base AS (
                SELECT 
                    split_part(vendor_code, '-', 1) AS md,
                    vendor_code AS mdc,
                    nm_id::bigint AS nm_id,
                    imt_id::bigint AS bundle_id
                FROM wb.cards
                --WHERE split_part(vendor_code, '-', 1) = 'RAB06W'
                ORDER BY vendor_code
                --LIMIT 50
            )
            SELECT 
                md,
                COUNT(DISTINCT bundle_id) AS bundle_count,
                COUNT(*) AS total_mdc,
                ARRAY_AGG(DISTINCT bundle_id) AS bundle_ids,
                ARRAY_AGG(DISTINCT mdc) AS vendor_codes,
                ARRAY_AGG(DISTINCT nm_id) AS nm_ids
            FROM base
            GROUP BY md
            HAVING COUNT(DISTINCT bundle_id) > 1
        """
        logger.info("Проверяю наличие разъединенных карточек в рамках одной модели")
        self.df = pd.read_sql(self.query, from_db)

    def update_cards(self):
        try:
            self._get_data(self.from_db)
            logger.info("Обрабатываю данные по строкам")
            for index, row in self.df.iterrows():
                data = {"targetIMT": row["bundle_ids"][0], "nmIDs": row["nm_ids"]}
                logger.info(f"Объединяю {row['md']} - {data}")
                try:
                    resp = self.api_client.post_connect_cards(data)
                except Exception as e:
                    logger.error(f"Ошибка объединения карточки {row['md']} - {e}")
                logger.info(f"Ответ сервера - {resp.status_code} - {resp.json()}")
                time.sleep(1)

        except Exception as e:
            logger.exception("Ошибка при обновлении major")
            self._send_telegram(f"❌ Ошибка объединения карточек: {e}")
            raise
        else:
            self._send_telegram("✅ Карточки успешно обновлены")
