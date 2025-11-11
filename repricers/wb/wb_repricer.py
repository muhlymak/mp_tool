import pandas as pd
from loguru import logger


def prepare_reprice_data(file) -> dict:
    """
    Принимает XLSX-файл с колонками: mdc, nm_id, price, discount.
    Возвращает словарь в формате:
    {
        "data": [
            {"nmID": 123, "price": 999, "discount": 30},
            ...
        ]
    }
    """
    logger.info(f"Подготавливаем данные для обновления цен, обрабатываю файл {file}")
    # Загружаем файл через pandas
    df = pd.read_excel(file, sheet_name="main")

    # Проверяем наличие нужных колонок
    required_columns = {"mdc", "nm_id", "price", "discount"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Отсутствуют обязательные колонки: {', '.join(missing)}")

    # Формируем список словарей
    data = (
        df[["nm_id", "price", "discount"]]
        .rename(columns={"nm_id": "nmID"})
        .to_dict(orient="records")
    )

    return {"data": data}