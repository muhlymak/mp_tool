import polars as pl
from loguru import logger
from psycopg2.extras import execute_values


def read_and_update_voronka_wb(
    xlsx_path,
    engine,
    sheet_name="Товары",
):
    logger.info("Читаем Excel...")
    df = pl.read_excel(
        source=xlsx_path,
        sheet_name=sheet_name,
        read_options={"header_row": 1},
        schema_overrides={
            "Артикул WB": pl.Utf8,
            "Дата": pl.Utf8,
            "Показы": pl.Int64,
            "Ярлыки": pl.Utf8,
        },
    )

    df = df.select(["Артикул WB", "Дата", "Показы"])
    df = df.rename({"Артикул WB": "nm_id", "Дата": "date", "Показы": "views"})

    logger.info(f"Загружено {df.height} строк из Excel")
    logger.info("Обновляем данные в таблице voronka...")

    pdf = df.to_pandas()
    data = pdf[["views", "nm_id", "date"]].to_dict(orient="records")
    values = [(row["nm_id"], row["date"], row["views"]) for row in data]

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        execute_values_sql = """
            UPDATE wb.customer_journey AS c
            SET views = v.views
            FROM (VALUES %s) AS v(nm_id, date, views)
            WHERE c.nm_id = v.nm_id AND c.date = v.date::date
        """
        execute_values(cur, execute_values_sql, values, template=None, page_size=1000)
        raw_conn.commit()
    finally:
        cur.close()
        raw_conn.close()

    logger.success("✅ Данные успешно обновлены в таблице voronka")
