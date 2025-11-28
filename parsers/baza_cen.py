import pandas as pd
from loguru import logger
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from helpers.telegram import send_telegram_message

class BazaCenUploader:
    def __init__(self, from_db, to_db):
        self.from_db = from_db
        self.to_db = to_db


    def _get_data(self, from_db):
        """
        Забираем данные по ценам из базы Visiology
        """
        self.query = """
            WITH pricing AS (
                SELECT 
                    DISTINCT(pr.MDK) AS mdc, 
                    pr.Brand AS brand_name, 
                    Season AS season_code, 
                    Department AS department_name, 
                    Class AS mainclass_name, 
                    Subclass AS subclass_name, 
                    Intake AS intake, 
                    CASE ReasonforPriceChange
                        WHEN 'Красный ценник'   THEN 'Clearance'
                        WHEN 'Регулярная цена'  THEN 'Regular'
                        WHEN 'Специальная цена' THEN 'Special price'
                        ELSE ReasonforPriceChange 
                    END AS price_type,
                    CASE
                        WHEN PriceZone IS NULL OR PriceZone = '' THEN NULL
                        ELSE SUBSTRING(
                                PriceZone,
                                CHARINDEX(' ', PriceZone) + 1,
                                CHARINDEX(' ', PriceZone, CHARINDEX(' ', PriceZone) + 1) 
                                - CHARINDEX(' ', PriceZone) - 1
                            )
                    END AS PriceZoneBrand,
                    TRY_CAST(REPLACE(REPLACE(Price, CHAR(160), ''), ',', '.') AS DECIMAL(18,2)) AS price_retail,
                    TRY_CAST(REPLACE(REPLACE(CostRUB, CHAR(160), ''), ',', '.') AS DECIMAL(18,2)) AS cost
                FROM dbo.TablePricing AS pr
                LEFT JOIN (
                    SELECT MDK, CostRUB
                    FROM (
                        SELECT 
                            MDK,
                            CostRUB,
                            ROW_NUMBER() OVER (PARTITION BY MDK ORDER BY CostRUB DESC) AS rn
                        FROM TableCostFiles tcf
                        WHERE DateUP = (SELECT MAX(DateUP) FROM TableCostFiles)
                    ) t
                    WHERE rn = 1
                ) cost  
                    ON cost.MDK = pr.MDK
                WHERE PricesOnDate >= CAST(GETDATE() AS DATE)
                AND RIGHT(PriceZone, 3) = 'ПЗ1'
                AND pr.MDK NOT IN ('YW607-01X')
            )
            SELECT 
                mdc,
                brand_name,
                season_code,
                department_name,
                mainclass_name,
                subclass_name,
                intake,
                price_type,
                price_retail,
                cost
            FROM pricing
            WHERE PriceZoneBrand = brand_name
        """
        logger.info('Забираем данные по ценам из базы Visiology')
        self.df = pd.read_sql(self.query, from_db)
        self.df['price_retail'] = pd.to_numeric(self.df['price_retail'], errors='coerce').fillna(0).astype(int)
        self.df['cost'] = pd.to_numeric(self.df['cost'], errors='coerce').fillna(0)




    def _upload_data(self, to_db, batch_size=10000):
        logger.info('Загружаем данные в базу MP батчами')

        meta = sa.MetaData()
        table = sa.Table("baza_cen", meta, autoload_with=to_db)

        records = self.df.to_dict(orient="records")

        with to_db.begin() as conn:
            for i in range(0, len(records), batch_size):
                chunk = records[i:i + batch_size]

                stmt = insert(table).values(chunk)

                # формируем UPSERT
                update_dict = {
                    c.name: stmt.excluded[c.name]
                    for c in table.columns
                    if c.name != "mdc"
                }

                stmt = stmt.on_conflict_do_update(
                    index_elements=["mdc"],
                    set_=update_dict
                )

                conn.execute(stmt)
                logger.info(f'Загружен батч {i // batch_size + 1}: {len(chunk)} строк')

        logger.success('Все данные успешно загружены в базу MP')



    def upload_data2(self, to_db):
        from sqlalchemy.dialects.postgresql import insert
        # Получаем метаданные таблицы
        from sqlalchemy import Table, MetaData

        metadata = MetaData()  # bind больше не нужен

        items_table = Table(
            'baza_cen', 
            metadata, 
            autoload_with=to_db  # сюда передаем engine/connection
        )

        # Перебираем строки DataFrame и делаем UPSERT
        with to_db.connect() as conn:
            for row in self.df.to_dict(orient='records'):
                stmt = insert(items_table).values(**row)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['mdc'],  # поле уникального ключа
                    set_={col: stmt.excluded[col] for col in row if col != 'mdc'}
                )
                conn.execute(stmt)
            conn.commit()


   
    def _send_telegram(self, message: str):
        send_telegram_message(message)

    def update_baza_cen(self):
        try:
            self._get_data(self.from_db)
            self._upload_data(self.to_db)
        except Exception as e:
            logger.exception("Ошибка при обновлении baza_cen")
            self._send_telegram(f"❌ Ошибка обновления baza_cen: {e}")
            raise
        else:
            self._send_telegram("✅ Данные baza_cen обновлены успешно")

