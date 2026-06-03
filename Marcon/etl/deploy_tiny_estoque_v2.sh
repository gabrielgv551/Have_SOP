#!/bin/bash
# Script de deploy para rodar tiny_estoque_v2.py no servidor
# Coloque este script no servidor via SCP e execute

cd /root || cd /home/ubuntu

# Instalar dependências se necessário
pip3 install requests sqlalchemy psycopg2-binary 2>/dev/null || pip install requests sqlalchemy psycopg2-binary

# Criar diretório para o script
mkdir -p /opt/tiny_estoque

# O script tiny_estoque_v2.py deve estar no mesmo diretório
# Execute: python3 /opt/tiny_estoque/tiny_estoque_v2.py --token "SEU_TOKEN"
