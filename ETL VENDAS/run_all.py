"""
ETL Hub — Roda todos os marketplaces em sequência
==================================================
USO:
  python run_all.py                  # todos ativos
  python run_all.py mercadolivre     # só ML
  python run_all.py shopee tiny      # Shopee + Tiny
"""

import sys
from datetime import datetime


def _sep(): print("─" * 60)


MODULOS = {
    "mercadolivre": {
        "label":  "Mercado Livre",
        "modulo": "mercadolivre.etl",
        "ativo":  True,
    },
    "shopee": {
        "label":  "Shopee",
        "modulo": "shopee.etl",
        "ativo":  False,      # ativar quando configurado
    },
    "amazon": {
        "label":  "Amazon",
        "modulo": "amazon.etl",
        "ativo":  False,
    },
    "tiny": {
        "label":  "Tiny ERP",
        "modulo": "tiny.etl",
        "ativo":  False,
    },
}


def rodar(nome: str, cfg: dict):
    print(f"\n  ▶  {cfg['label']}")
    _sep()
    try:
        import importlib
        mod = importlib.import_module(cfg["modulo"])
        mod.main()
    except ModuleNotFoundError:
        print(f"  [!] {cfg['label']}: módulo não implementado ainda.")
    except Exception as e:
        print(f"  [ERRO] {cfg['label']}: {e}")


if __name__ == "__main__":
    inicio = datetime.now()
    print("\n" + "═" * 60)
    print("  ETL Hub — Todos os Marketplaces")
    print(f"  {inicio:%d/%m/%Y %H:%M:%S}")
    print("═" * 60)

    filtros = [a.lower() for a in sys.argv[1:]]

    for nome, cfg in MODULOS.items():
        if filtros and nome not in filtros:
            continue
        if not filtros and not cfg["ativo"]:
            print(f"\n  ⏭  {cfg['label']} — inativo (configure e ative no run_all.py)")
            continue
        rodar(nome, cfg)

    duracao = int((datetime.now() - inicio).total_seconds())
    print(f"\n{'═'*60}")
    print(f"  ✔  Concluído em {duracao//60}m{duracao%60:02d}s")
    print("═" * 60)

    try:
        from consolidar import status_consolidado
        status_consolidado()
    except Exception as _e:
        print(f"  [aviso] consolidado: {_e}")
