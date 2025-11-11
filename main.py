from parsers.baza_cen import BazaCenUploader
from db.connections import mp_engine, visiology_engine
import typer
from loguru import logger

from parsers.voronka_wb import read_and_update_voronka_wb
from repricers.lm.api_client import LmApiClient
from repricers.lm.lm_repricer import prepare_lamoda_price_payload
from repricers.wb.api_client import WbApiClient
from repricers.wb.wb_repricer import prepare_reprice_data

app = typer.Typer(help="CLI утилита для загрузки данных в базу MP")



@app.command("bc-update")
def baza_cen_update():
    """Загружаем базу цен из Visiology и обновляем MP базу"""
    uploader = BazaCenUploader(from_db=visiology_engine, to_db=mp_engine)
    uploader.update_baza_cen()
    typer.echo("✅ Данные успешно обновлены в базе MP.")


@app.command("wb-voronka-update")
def update_voronka_wb():
    """Перекидываем показы по воронке из выгрузки из кабинета в БД"""
    read_and_update_voronka_wb(xlsx_path="data/voronka.xlsx", engine=mp_engine)

@app.command("wb-reprice")
def reprice_wb():
    """Перекидываем цены из файла в API WB"""
    data = prepare_reprice_data(file="data/wb/reprice.xlsx")
    logger.info("Инициализирую клиента WB API...")
    wb_api_client = WbApiClient()
    logger.info("Отправляю запрос на переоценку...")
    response = wb_api_client.post_reprice(data=data)
    logger.info(
        f"Запрос на переоценку отправлен. Статус код: {response.status_code} - "
        f"Тело ответа: {response.json()}"
    )

@app.command("lm_repice")
def reprice_lm():
    api_cleient = LmApiClient()
    data = prepare_lamoda_price_payload(xlsx_path="data/lm/reprice.xlsx")
    response = api_cleient.post_reprice(data).json()
    logger.info(f"Запрос на переоценку отправлен - успешно обработано: {response['successCount']}")
    logger.info(f"Количество ошибок: {response['errorCount']}")
    if response["errorCount"] > 0:
        logger.error(f"Список ошибок: {response['errors']}")


if __name__ == "__main__":
    app()