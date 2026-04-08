-- Tabela de configuração dos módulos S&OP
CREATE TABLE IF NOT EXISTS sopc_config (
  empresa  VARCHAR(50)  NOT NULL,
  modulo   VARCHAR(50)  NOT NULL,
  chave    VARCHAR(100) NOT NULL,
  valor    TEXT         NOT NULL,
  PRIMARY KEY (empresa, modulo, chave)
);

-- ── Curva ABC ─────────────────────────────────────
INSERT INTO sopc_config (empresa, modulo, chave, valor) VALUES
  ('lanzi', 'curva_abc', 'janela_meses',       '6'),
  ('lanzi', 'curva_abc', 'corte_a',            '0.20'),
  ('lanzi', 'curva_abc', 'corte_b',            '0.50'),
  ('lanzi', 'curva_abc', 'nivel_servico_AA',   '0.98'),
  ('lanzi', 'curva_abc', 'nivel_servico_AB',   '0.97'),
  ('lanzi', 'curva_abc', 'nivel_servico_BA',   '0.97'),
  ('lanzi', 'curva_abc', 'nivel_servico_BB',   '0.95'),
  ('lanzi', 'curva_abc', 'nivel_servico_AC',   '0.95'),
  ('lanzi', 'curva_abc', 'nivel_servico_CA',   '0.95'),
  ('lanzi', 'curva_abc', 'nivel_servico_BC',   '0.92'),
  ('lanzi', 'curva_abc', 'nivel_servico_CB',   '0.92'),
  ('lanzi', 'curva_abc', 'nivel_servico_CC',   '0.90')
ON CONFLICT (empresa, modulo, chave) DO NOTHING;

-- ── Estoque de Segurança ──────────────────────────
INSERT INTO sopc_config (empresa, modulo, chave, valor) VALUES
  ('lanzi', 'estoque_seg', 'janela_meses',  '12'),
  ('lanzi', 'estoque_seg', 'fator_z_AA',    '2.05'),
  ('lanzi', 'estoque_seg', 'fator_z_AB',    '1.88'),
  ('lanzi', 'estoque_seg', 'fator_z_BA',    '1.88'),
  ('lanzi', 'estoque_seg', 'fator_z_BB',    '1.65'),
  ('lanzi', 'estoque_seg', 'fator_z_AC',    '1.65'),
  ('lanzi', 'estoque_seg', 'fator_z_CA',    '1.65'),
  ('lanzi', 'estoque_seg', 'fator_z_BC',    '1.41'),
  ('lanzi', 'estoque_seg', 'fator_z_CB',    '1.41'),
  ('lanzi', 'estoque_seg', 'fator_z_CC',    '1.28'),
  ('lanzi', 'estoque_seg', 'teto_dias_A',   '20'),
  ('lanzi', 'estoque_seg', 'teto_dias_BC',  '15')
ON CONFLICT (empresa, modulo, chave) DO NOTHING;

-- ── Previsão 12M ──────────────────────────────────
INSERT INTO sopc_config (empresa, modulo, chave, valor) VALUES
  ('lanzi', 'prev_12m', 'blend_longo',        '0.40'),
  ('lanzi', 'prev_12m', 'blend_curto',        '0.60'),
  ('lanzi', 'prev_12m', 'peso_t_minus2',      '1'),
  ('lanzi', 'prev_12m', 'peso_t_minus1',      '2'),
  ('lanzi', 'prev_12m', 'peso_t',             '4'),
  ('lanzi', 'prev_12m', 'min_meses_grupo_a',  '6')
ON CONFLICT (empresa, modulo, chave) DO NOTHING;

-- ── Ponto de Pedido ───────────────────────────────
INSERT INTO sopc_config (empresa, modulo, chave, valor) VALUES
  ('lanzi', 'ponto_pedido', 'horizonte_demanda_dias', '90'),
  ('lanzi', 'ponto_pedido', 'ciclo_reposicao_dias',   '30'),
  ('lanzi', 'ponto_pedido', 'fator_excesso',          '2.0')
ON CONFLICT (empresa, modulo, chave) DO NOTHING;
