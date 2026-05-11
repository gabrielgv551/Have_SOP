"""
Conexão compartilhada com o banco de dados.
Importada por todos os módulos de ETL.
"""

import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        url = (
            f"postgresql+psycopg2://{os.getenv('DB_USER', 'postgres')}"
            f":{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST', '37.60.236.200')}"
            f":{os.getenv('DB_PORT', '5432')}"
            f"/{os.getenv('DB_NAME', 'Marcon')}"
        )
        _engine = create_engine(url, pool_pre_ping=True, pool_size=5)
    return _engine
