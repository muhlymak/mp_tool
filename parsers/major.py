import pandas as pd
from loguru import logger
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from helpers.telegram import send_telegram_message

class MajorUploader:
    def __init__(self, from_db, to_db):
        self.from_db = from_db
        self.to_db = to_db

    def _send_telegram(self, message: str):
        send_telegram_message(message)


    def _get_data(self, from_db):
        """
        Забираем данные по стоку майора из базы Visiology
        """
        self.query = """
            SELECT 
            CAST(GETDATE() AS DATE) AS date,
            Brand AS brand,
            Season AS season,
            Department AS department,
            Class AS class,
            Subclass AS subclass,
            Collection AS collection,
            Model AS model,
            MDK AS mdc,
            Article AS article,
            Barcode AS barcode,
            Nomenclature AS nomenclature,
            Intake AS intake,
            Prepack AS prepack,
            TRY_CAST(REPLACE(REPLACE(mj.QuantityInPack, CHAR(160), ''), ' ', '') AS FLOAT) AS quantity_in_pack,
            TRY_CAST(REPLACE(REPLACE(mj.Total, CHAR(160), ''), ' ', '') AS FLOAT) AS total
            FROM [VisiologyDataStore].[dbo].[DailyRemaining28Major] AS mj
            WHERE mj.[Date] >= CAST(GETDATE() AS DATE)
            --WHERE mj.[Date] >= CAST('2025-11-26' AS DATE)
        """
        logger.info('Забираем данные по стоку майора из базы Visiology')
        self.df = pd.read_sql(self.query, from_db)


    def _upload_data(self, to_db, batch_size=5000):
        logger.info('Загружаем данные в базу MP батчами')

        if self.df is None or self.df.empty:
            logger.warning("DataFrame major пустой — загрузка не выполняется")
            raise ValueError("DataFrame major пустой — загрузка не выполняется")

        meta = sa.MetaData()
        table = sa.Table("major", meta, autoload_with=to_db)

        records = self.df.to_dict(orient="records")

        with to_db.begin() as conn:
            # 1. Очищаем таблицу в той же транзакции
            logger.info("Очищаем таблицу major перед загрузкой")
            conn.execute(sa.text("DELETE FROM major"))

            # 2. Загружаем данные батчами
            for i in range(0, len(records), batch_size):
                chunk = records[i:i + batch_size]
                stmt = insert(table)
                conn.execute(stmt, chunk)
                logger.info(f'Загружен батч {i // batch_size + 1}: {len(chunk)} строк')

        logger.success('Все данные успешно загружены в базу MP')



    def update_major(self):
        try:
            self._get_data(self.from_db)
            self._upload_data(self.to_db)
        except Exception as e:
            logger.exception("Ошибка при обновлении major")
            self._send_telegram(f"❌ Ошибка обновления major: {e}")
            raise
        else:
            self._send_telegram("✅ Данные major обновлены успешно")
