"""
╔══════════════════════════════════════════════════════════════╗
║           S&OP Intelligence · Execução Completa              ║
║  Roda todos os scripts na ordem correta                      ║
╚══════════════════════════════════════════════════════════════╝

ORDEM DE EXECUÇÃO:
  1. UPLOAD_ETL.py       → Carrega Excel para o banco
  2. PREVISÃO 12M.py     → Gera previsão de demanda 12 meses
  3. Curva_ABC.PY        → Calcula curva ABC dinâmica
  4. Estoque_Seguranca.py → Calcula estoque de segurança por SKU
  5. Ponto_Pedido.py     → Calcula ponto de pedido + lista semanal
  6. PPR_SKU.py          → Performance de vendas por janela temporal
"""

import subprocess
import sys
import os
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURAÇÃO — ajuste o caminho se necessário
# ─────────────────────────────────────────────
PASTA = os.path.dirname(os.path.abspath(__file__))

SCRIPTS = [
    ("1/6", "UPLOAD_ETL.py",        "Carregando Excel para o banco..."),
    ("2/6", "PREVISÃO 12M.py",      "Gerando Previsão de Demanda 12M..."),
    ("3/6", "Curva_ABC.PY",         "Calculando Curva ABC..."),
    ("4/6", "Estoque_Seguranca.py", "Calculando Estoque de Segurança..."),
    ("5/6", "Ponto_Pedido.py",      "Calculando Ponto de Pedido + Lista Semanal..."),
    ("6/6", "PPR_SKU.py",           "Calculando Performance de Vendas por Janela (PPR)..."),
]


def separador():
    print("=" * 60)


def rodar_script(passo, arquivo, descricao):
    caminho = os.path.join(PASTA, arquivo)

    separador()
    print(f"  [{passo}] {descricao}")
    print(f"         Arquivo: {arquivo}")
    print(f"         Início : {datetime.now().strftime('%H:%M:%S')}")
    separador()

    if not os.path.exists(caminho):
        print(f"\n  [ERRO] Arquivo não encontrado: {caminho}")
        print("         Verifique se todos os scripts estão na mesma pasta.\n")
        return False

    resultado = subprocess.run(
        [sys.executable, caminho],
        cwd=PASTA
    )

    if resultado.returncode != 0:
        print(f"\n  [ERRO] '{arquivo}' terminou com erro (código {resultado.returncode}).")
        print("         Corrija o erro acima antes de continuar.\n")
        return False

    print(f"\n  [OK] '{arquivo}' finalizado com sucesso!")
    print(f"       Fim: {datetime.now().strftime('%H:%M:%S')}\n")
    return True


def main():
    inicio = datetime.now()

    separador()
    print("  S&OP Intelligence · Execução Completa")
    print(f"  Início: {inicio.strftime('%d/%m/%Y %H:%M:%S')}")
    separador()
    print()

    for passo, arquivo, descricao in SCRIPTS:
        sucesso = rodar_script(passo, arquivo, descricao)
        if not sucesso:
            print("\n[INTERROMPIDO] Pipeline parado por erro.")
            print("Corrija o problema e rode novamente.\n")
            sys.exit(1)

    fim = datetime.now()
    duracao = (fim - inicio).seconds

    separador()
    print("  [OK] PIPELINE COMPLETO!")
    print(f"  Início : {inicio.strftime('%H:%M:%S')}")
    print(f"  Fim    : {fim.strftime('%H:%M:%S')}")
    print(f"  Duração: {duracao // 60}min {duracao % 60}s")
    separador()


if __name__ == "__main__":
    main()