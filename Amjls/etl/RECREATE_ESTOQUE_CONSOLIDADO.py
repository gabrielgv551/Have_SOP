#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para recriar a tabela estoque_consolidado no PostgreSQL
Útil quando o arquivo Excel não está disponível
"""

import psycopg2
import os

DB_CONFIG = {
    "host"    : os.getenv("AMJLS_HOST", "37.60.236.200"),
    "port"    : os.getenv("AMJLS_PORT", 5432),
    "database": os.getenv("AMJLS_DB", "amjls"),
    "user"    : os.getenv("AMJLS_USER", "postgres"),
    "password": os.getenv("AMJLS_PASSWORD", "131105Gv"),
}

try:
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )
    cursor = conn.cursor()
    
    print("[...] Criando tabela estoque_consolidado...")
    
    # Dropar tabela se existir
    cursor.execute('DROP TABLE IF EXISTS estoque_consolidado CASCADE')
    print("     Tabela anterior removida (se existia)")
    
    # Criar tabela com estrutura igual à da Lanzi
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS estoque_consolidado (
            id SERIAL PRIMARY KEY,
            "SKU" VARCHAR(100) NOT NULL,
            "Produto" VARCHAR(255),
            "Estoque Base" DECIMAL(15, 2) DEFAULT 0,
            "Origem" VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Criar índices para melhor performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_estoque_sku ON estoque_consolidado("SKU")')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_estoque_origem ON estoque_consolidado("Origem")')
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("[OK] Tabela estoque_consolidado criada com sucesso!")
    print("    Estrutura: SKU + Produto + Estoque Base + Origem")
    print("\n⚠️  PRÓXIMO PASSO: Populate a tabela com dados reais")
    print("    Opção 1: Execute UPLOAD_ETL.py com o arquivo Excel")
    print("    Opção 2: Importe dados manualmente via SQL INSERT")
    
except psycopg2.Error as e:
    print(f"[ERRO] Falha ao conectar ao banco: {e}")
except Exception as e:
    print(f"[ERRO] {e}")
