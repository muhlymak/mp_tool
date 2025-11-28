import os
import pandas as pd
from datetime import datetime, timedelta, timezone


def prepare_lamoda_price_payload(xlsx_path: str, start_date: str | None = None) -> dict:
    """
    Формирует словарь для Lamoda API из XLSX-файла с колонками:
    mdc, nm_id, price, sale_price

    Если sale_price не указан, то поле sale_price и даты не добавляются.
    """
    # Читаем Excel
    df = pd.read_excel(xlsx_path, sheet_name="main", dtype={"mdc": str, "nm_id": str})

    # Определяем даты по умолчанию (МСК)
    msk = timezone(timedelta(hours=3))
    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=msk)
    else:
        start_dt = datetime.now(msk).replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = start_dt + timedelta(days=200)

    # Конвертируем в ISO 8601 формат
    start_iso = start_dt.isoformat()
    end_iso = end_dt.isoformat()

    items = []

    for _, row in df.iterrows():
        price = float(row["price"]) if pd.notna(row["price"]) else None
        sale_price = row.get("sale_price")

        item = {
            "price": price,
            "parent_sku": str(row["nm_id"]),
        }

        # Добавляем скидку и даты только если sale_price указана
        if pd.notna(sale_price) and sale_price != "":
            item.update({
                "sale_price": float(sale_price),
                "sale_start_date": start_iso,
                "sale_end_date": end_iso,
            })

        items.append(item)

    return {
        "items": items,
        "partner_id": os.getenv("lm_partner_id"),
    }
