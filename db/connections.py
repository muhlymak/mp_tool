from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv()


visiology_engine = create_engine(
    f"mssql+pyodbc://"
    f"{os.getenv('visiology_db_user')}:"
    f"{os.getenv('visiology_db_password')}@"
    f"{os.getenv('visiology_db_server')}/"
    f"{os.getenv('visiology_db_name')}?"
    f"driver=SQL+Server"
)

mp_engine = create_engine(
    f"postgresql+psycopg2://"
    f"{os.getenv('mp_db_user')}:"
    f"{os.getenv('mp_db_password')}@"
    f"{os.getenv('mp_db_server')}:"
    f"{os.getenv('mp_db_port')}/"
    f"{os.getenv('mp_db_name')}"
)
