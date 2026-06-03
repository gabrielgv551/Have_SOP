#!/bin/bash
# Wrapper para rodar o extrator com display virtual (xvfb)
# O GCOM Web detecta navegador headless e bloqueia o executeRule
# xvfb simula uma tela real, permitindo headless=False no servidor

cd /opt/scripts
xvfb-run -a --server-args="-screen 0 1280x720x24" /usr/bin/python3 extrair_contas_a_pagar.py
