import pandas as pd
from loguru import logger
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from helpers.telegram import send_telegram_message

class StoresStockUploader:
    def __init__(self, from_db, to_db):
        self.from_db = from_db
        self.to_db = to_db

    def _send_telegram(self, message: str):
        send_telegram_message(message)


    def _get_data(self, from_db):
        """
        Забираем данные по стоку магазинов из базы Visiology
        """
        self.query = """
            SELECT 
                CAST(GETDATE() AS DATE) AS date,
                MDK AS mdc, 
                WarehouseNumber AS store_number, 
                SUM(InitialStatus) AS eop_u
            FROM VisiologyDataStore.dbo.BalancesDaysWarehouses
            WHERE EndPeriod >= DATEADD(day, DATEDIFF(day, 1, GETDATE()), 0)
            AND EndPeriod <  DATEADD(day, DATEDIFF(day, 0, GETDATE()), 0)
            GROUP BY 
                MDK, 
                WarehouseNumber
        """
        logger.info('Забираем данные по стоку магазинов из базы Visiology')
        self.df = pd.read_sql(self.query, from_db)


    def _upload_data(self, to_db, batch_size=20000):
        logger.info('Загружаем данные в базу MP батчами')

        if self.df is None or self.df.empty:
            message = "DataFrame stores stock пустой — загрузка не выполняется"
            logger.warning(message)
            raise ValueError(message)

        meta = sa.MetaData()
        table = sa.Table("stores_stock", meta, autoload_with=to_db)

        records = self.df.to_dict(orient="records")

        with to_db.begin() as conn:
            # 1. Очищаем таблицу в той же транзакции
            logger.info("Очищаем таблицу stores_stock перед загрузкой")
            conn.execute(sa.text("DELETE FROM stores_stock"))

            # 2. Загружаем данные батчами
            for i in range(0, len(records), batch_size):
                chunk = records[i:i + batch_size]
                stmt = insert(table)
                conn.execute(stmt, chunk)
                logger.info(f'Загружен батч {i // batch_size + 1}: {len(chunk)} строк')

        logger.success('Все данные успешно загружены в базу MP')



    def update_stores_stock(self):
        try:
            self._get_data(self.from_db)
            self._upload_data(self.to_db)
        except Exception as e:
            logger.exception("Ошибка при обновлении stores stock")
            self._send_telegram(f"❌ Ошибка обновления major: {e}")
            raise
        else:
            self._send_telegram("✅ Данные stores stock обновлены успешно")
