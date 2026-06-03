-- Migration: Update calculation functions for dashboard KPIs
-- Date: 2026-05-29
-- Purpose: Add/update SQL functions for KPI calculations

-- ============================================================
-- Function: calcular_receita_liquida
-- Calcula receita líquida excluindo cancelamentos e devoluções
-- ============================================================
CREATE OR REPLACE FUNCTION calcular_receita_liquida(
  p_empresa VARCHAR(50),
  p_ano INTEGER,
  p_mes INTEGER
) RETURNS NUMERIC AS $$
BEGIN
  RETURN COALESCE(
    (SELECT SUM(COALESCE("Total Venda", 0))
     FROM bd_vendas
     WHERE empresa = p_empresa
       AND EXTRACT(YEAR FROM "Data"::date)::int = p_ano
       AND EXTRACT(MONTH FROM "Data"::date)::int = p_mes
       AND "Status" !~* '(cancel|devol|n[aã]o.?pago)'),
    0
  );
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================
-- Function: calcular_margem_bruta
-- Calcula margem bruta total do período
-- ============================================================
CREATE OR REPLACE FUNCTION calcular_margem_bruta(
  p_empresa VARCHAR(50),
  p_ano INTEGER,
  p_mes INTEGER
) RETURNS NUMERIC AS $$
BEGIN
  RETURN COALESCE(
    (SELECT SUM(COALESCE("Margem Produto", 0))
     FROM bd_vendas
     WHERE empresa = p_empresa
       AND EXTRACT(YEAR FROM "Data"::date)::int = p_ano
       AND EXTRACT(MONTH FROM "Data"::date)::int = p_mes
       AND "Status" !~* '(cancel|devol|n[aã]o.?pago)'),
    0
  );
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================
-- Function: calcular_margem_percentual
-- Calcula margem como percentual da receita líquida
-- ============================================================
CREATE OR REPLACE FUNCTION calcular_margem_percentual(
  p_empresa VARCHAR(50),
  p_ano INTEGER,
  p_mes INTEGER
) RETURNS NUMERIC AS $$
DECLARE
  v_receita_liquida NUMERIC;
  v_margem_bruta NUMERIC;
BEGIN
  v_receita_liquida := calcular_receita_liquida(p_empresa, p_ano, p_mes);
  v_margem_bruta := calcular_margem_bruta(p_empresa, p_ano, p_mes);
  
  IF v_receita_liquida > 0 THEN
    RETURN ROUND((v_margem_bruta / v_receita_liquida * 100)::numeric, 2);
  ELSE
    RETURN 0;
  END IF;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================
-- Function: calcular_ticket_medio
-- Calcula ticket médio (receita / quantidade)
-- ============================================================
CREATE OR REPLACE FUNCTION calcular_ticket_medio(
  p_empresa VARCHAR(50),
  p_ano INTEGER,
  p_mes INTEGER
) RETURNS NUMERIC AS $$
DECLARE
  v_receita_liquida NUMERIC;
  v_quantidade NUMERIC;
BEGIN
  v_receita_liquida := calcular_receita_liquida(p_empresa, p_ano, p_mes);
  
  v_quantidade := COALESCE(
    (SELECT SUM(COALESCE("Quantidade Vendida", 0))
     FROM bd_vendas
     WHERE empresa = p_empresa
       AND EXTRACT(YEAR FROM "Data"::date)::int = p_ano
       AND EXTRACT(MONTH FROM "Data"::date)::int = p_mes
       AND "Status" !~* '(cancel|devol|n[aã]o.?pago)'),
    0
  );
  
  IF v_quantidade > 0 THEN
    RETURN ROUND((v_receita_liquida / v_quantidade)::numeric, 2);
  ELSE
    RETURN 0;
  END IF;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================
-- Function: calcular_receita_bruta
-- Calcula receita bruta (total de vendas sem filtros)
-- ============================================================
CREATE OR REPLACE FUNCTION calcular_receita_bruta(
  p_empresa VARCHAR(50),
  p_ano INTEGER,
  p_mes INTEGER
) RETURNS NUMERIC AS $$
BEGIN
  RETURN COALESCE(
    (SELECT SUM(COALESCE("Total Venda", 0)) - SUM(COALESCE("Valor Desconto", 0))
     FROM bd_vendas
     WHERE empresa = p_empresa
       AND EXTRACT(YEAR FROM "Data"::date)::int = p_ano
       AND EXTRACT(MONTH FROM "Data"::date)::int = p_mes),
    0
  );
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================
-- Function: calcular_custo_total
-- Calcula custo total do período
-- ============================================================
CREATE OR REPLACE FUNCTION calcular_custo_total(
  p_empresa VARCHAR(50),
  p_ano INTEGER,
  p_mes INTEGER
) RETURNS NUMERIC AS $$
BEGIN
  RETURN COALESCE(
    (SELECT SUM(COALESCE("Custo Total", 0))
     FROM bd_vendas
     WHERE empresa = p_empresa
       AND EXTRACT(YEAR FROM "Data"::date)::int = p_ano
       AND EXTRACT(MONTH FROM "Data"::date)::int = p_mes
       AND "Status" !~* '(cancel|devol|n[aã]o.?pago)'),
    0
  );
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================
-- Function: get_dashboard_kpis
-- Retorna todos os KPIs do dashboard para um período
-- ============================================================
CREATE OR REPLACE FUNCTION get_dashboard_kpis(
  p_empresa VARCHAR(50),
  p_ano INTEGER DEFAULT NULL,
  p_mes INTEGER DEFAULT NULL
) RETURNS TABLE (
  ano INTEGER,
  mes INTEGER,
  receita_bruta NUMERIC,
  receita_liquida NUMERIC,
  qtd_liquida NUMERIC,
  margem_bruta NUMERIC,
  margem_pct NUMERIC,
  ticket_medio NUMERIC,
  custo_total NUMERIC
) AS $$
DECLARE
  v_ano INTEGER;
  v_mes INTEGER;
BEGIN
  -- Se não fornecido, usar mês/ano atual
  IF p_ano IS NULL OR p_mes IS NULL THEN
    v_ano := EXTRACT(YEAR FROM NOW())::int;
    v_mes := EXTRACT(MONTH FROM NOW())::int;
  ELSE
    v_ano := p_ano;
    v_mes := p_mes;
  END IF;
  
  RETURN QUERY
  SELECT
    v_ano,
    v_mes,
    calcular_receita_bruta(p_empresa, v_ano, v_mes),
    calcular_receita_liquida(p_empresa, v_ano, v_mes),
    COALESCE(
      (SELECT SUM(COALESCE("Quantidade Vendida", 0))
       FROM bd_vendas
       WHERE empresa = p_empresa
         AND EXTRACT(YEAR FROM "Data"::date)::int = v_ano
         AND EXTRACT(MONTH FROM "Data"::date)::int = v_mes
         AND "Status" !~* '(cancel|devol|n[aã]o.?pago)'),
      0
    ),
    calcular_margem_bruta(p_empresa, v_ano, v_mes),
    calcular_margem_percentual(p_empresa, v_ano, v_mes),
    calcular_ticket_medio(p_empresa, v_ano, v_mes),
    calcular_custo_total(p_empresa, v_ano, v_mes);
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================
-- Function: get_monthly_kpis
-- Retorna KPIs mensais para série temporal
-- ============================================================
CREATE OR REPLACE FUNCTION get_monthly_kpis(
  p_empresa VARCHAR(50),
  p_ano_inicio INTEGER DEFAULT 2024,
  p_ano_fim INTEGER DEFAULT NULL
) RETURNS TABLE (
  ano INTEGER,
  mes INTEGER,
  receita_bruta NUMERIC,
  receita_liquida NUMERIC,
  qtd_liquida NUMERIC,
  margem_bruta NUMERIC,
  margem_pct NUMERIC
) AS $$
DECLARE
  v_ano_fim INTEGER;
  v_ano INTEGER;
  v_mes INTEGER;
BEGIN
  v_ano_fim := COALESCE(p_ano_fim, EXTRACT(YEAR FROM NOW())::int);
  
  v_ano := p_ano_inicio;
  WHILE v_ano <= v_ano_fim LOOP
    v_mes := 1;
    WHILE v_mes <= 12 LOOP
      RETURN QUERY
      SELECT
        v_ano,
        v_mes,
        calcular_receita_bruta(p_empresa, v_ano, v_mes),
        calcular_receita_liquida(p_empresa, v_ano, v_mes),
        COALESCE(
          (SELECT SUM(COALESCE("Quantidade Vendida", 0))
           FROM bd_vendas
           WHERE empresa = p_empresa
             AND EXTRACT(YEAR FROM "Data"::date)::int = v_ano
             AND EXTRACT(MONTH FROM "Data"::date)::int = v_mes
             AND "Status" !~* '(cancel|devol|n[aã]o.?pago)'),
          0
        ),
        calcular_margem_bruta(p_empresa, v_ano, v_mes),
        calcular_margem_percentual(p_empresa, v_ano, v_mes);
      
      v_mes := v_mes + 1;
    END LOOP;
    v_ano := v_ano + 1;
  END LOOP;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================
-- Grants para as funções (ajuste conforme necessário)
-- ============================================================
GRANT EXECUTE ON FUNCTION calcular_receita_liquida(VARCHAR, INTEGER, INTEGER) TO postgres;
GRANT EXECUTE ON FUNCTION calcular_margem_bruta(VARCHAR, INTEGER, INTEGER) TO postgres;
GRANT EXECUTE ON FUNCTION calcular_margem_percentual(VARCHAR, INTEGER, INTEGER) TO postgres;
GRANT EXECUTE ON FUNCTION calcular_ticket_medio(VARCHAR, INTEGER, INTEGER) TO postgres;
GRANT EXECUTE ON FUNCTION calcular_receita_bruta(VARCHAR, INTEGER, INTEGER) TO postgres;
GRANT EXECUTE ON FUNCTION calcular_custo_total(VARCHAR, INTEGER, INTEGER) TO postgres;
GRANT EXECUTE ON FUNCTION get_dashboard_kpis(VARCHAR, INTEGER, INTEGER) TO postgres;
GRANT EXECUTE ON FUNCTION get_monthly_kpis(VARCHAR, INTEGER, INTEGER) TO postgres;

SELECT 'Calculation functions updated successfully' AS status;
