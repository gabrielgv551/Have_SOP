"""
db_config.py — Configuração centralizada de bancos de dados.
Todos os scripts ETL devem importar daqui em vez de hardcodar credenciais.

Uso:
    from db_config import get_db_config, get_engine, get_conn

Credenciais lidas de variáveis de ambiente ou do arquivo .env na raiz.
"""
import os
from pathlib import Path

# Carrega .env se existir (sem precisar instalar python-dotenv)
_env_path = Path(__file__).parent / '.env'
if _env_path.exists():
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, _, v = line.partition('=')
                os.environ.setdefault(k.strip(), v.strip())


def get_db_config(empresa: str) -> dict:
    """Retorna dict de conexão para a empresa informada."""
    key = empresa.upper()
    return {
        "host"    : os.getenv(f"{key}_HOST",     "37.60.236.200"),
        "port"    : int(os.getenv(f"{key}_PORT", "5432")),
        "database": os.getenv(f"{key}_DB",       empresa),
        "user"    : os.getenv(f"{key}_USER",     "postgres"),
        "password": os.getenv(f"{key}_PASSWORD", ""),
    }


def get_engine(empresa: str):
    """Retorna SQLAlchemy engine para a empresa."""
    from sqlalchemy import create_engine
    c = get_db_config(empresa)
    url = f"postgresql+psycopg2://{c['user']}:{c['password']}@{c['host']}:{c['port']}/{c['database']}"
    return create_engine(url)


def get_conn(empresa: str):
    """Retorna conexão psycopg2 para a empresa."""
    import psycopg2
    return psycopg2.connect(**get_db_config(empresa))
