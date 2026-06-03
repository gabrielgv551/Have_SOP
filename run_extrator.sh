#!/bin/bash
# Wrapper que garante dependencias e roda o extrator

cd /opt/scripts

# Verifica se playwright esta instalado
if ! python3 -c "import playwright" 2>/dev/null; then
    echo "Playwright nao encontrado. Instalando..."
    python3 -m pip install playwright requests urllib3 --break-system-packages
    python3 -m playwright install chromium
    python3 -m playwright install-deps chromium
fi

# Roda o extrator
python3 extrair_contas_a_pagar.py
