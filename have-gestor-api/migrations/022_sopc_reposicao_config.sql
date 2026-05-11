-- Parâmetros de reposição S&OP por empresa (módulo 'reposicao')
-- Lidos dinamicamente pelo agente — alterar aqui muda o comportamento da IA

INSERT INTO sopc_config (empresa, modulo, chave, valor) VALUES
  -- Metas de cobertura por curva ABC (dias)
  ('lanzi', 'reposicao', 'meta_dias_a',            '20'),
  ('lanzi', 'reposicao', 'meta_dias_b',            '15'),
  ('lanzi', 'reposicao', 'meta_dias_c',            '10'),
  -- Thresholds de alerta em sopc_portfolio_saude
  ('lanzi', 'reposicao', 'alerta_ruptura_dias',    '7'),
  ('lanzi', 'reposicao', 'alerta_abaixo_meta_dias','15'),
  -- Threshold de encalhe (dias cobertura acima = risco de encalhe)
  ('lanzi', 'reposicao', 'encalhe_dias',           '90'),
  -- Lead time de reposição (dias)
  ('lanzi', 'reposicao', 'lead_time_dias',         '15')
ON CONFLICT (empresa, modulo, chave) DO UPDATE SET valor = EXCLUDED.valor;
