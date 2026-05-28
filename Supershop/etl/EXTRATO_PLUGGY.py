"""
╔══════════════════════════════════════════════════════════════╗
║      Extrato ETL · Supershop                                 ║
║  Puxa transações do banco "extratos" (Extrator Bancários)    ║
║  e sincroniza para a tabela caixa_extrato do Supershop       ║
╚══════════════════════════════════════════════════════════════╝

Vincule o cliente no app: extrator-bancario.vercel.app
  → Abra o cliente → clique em "Vincular ao Gestor..." → selecione "supershop"

Dependências:
  pip install psycopg2-binary python-dotenv

Uso:
  python EXTRATO_PLUGGY.py              → incremental desde último sync
  $env:FULL_RELOAD='1'; python EXTRATO_PLUGGY.py → recarrega desde FIRST_DATE
"""

import sys
from pathlib import Path

# ─── Carrega .env e adiciona etl_common ao path ──────────────────
_root = Path(__file__).resolve().parent.parent.parent
_env_path = _root / ".env"
if _env_path.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(str(_env_path))
    except ImportError:
        pass

sys.path.insert(0, str(_root))
from etl_common.pluggy_extrato import run

if __name__ == "__main__":
    run("supershop")
