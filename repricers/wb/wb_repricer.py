import pandas as pd
from loguru import logger


def prepare_reprice_data(file) -> list[dict]:
    """
    Принимает XLSX-файл с колонками: mdc, nm_id, price, discount.
    Возвращает список словарей формата:
    [
        {"nmID": 123, "price": 999, "discount": 30},
        ...
    ]
    """
    logger.info(f"Подготавливаем данные: {file}")

    df = pd.read_excel(file, sheet_name="main")

    required_columns = {"mdc", "nm_id", "price", "discount"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Отсутствуют обязательные колонки: {', '.join(missing)}")

    data = (
        df[["nm_id", "price", "discount"]]
        .rename(columns={"nm_id": "nmID"})
        .to_dict(orient="records")
    )

    return data


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]