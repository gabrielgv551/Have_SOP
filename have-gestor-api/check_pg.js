const { Pool } = require('pg');

const pool = new Pool({
  connectionString: 'postgres://postgres:131105Gv@37.60.236.200:5432/Lanzi'
});

async function run() {
  try {
    const res = await pool.query(
      `SELECT e.id::text AS id, e.dia, e.descricao, e.razao_social, e.account_number,
              e.counterparty_document, e.valor,
              e.belvo_tx_id, e.banco_id, b.nome AS banco_nome,
              e.atualizado_em
       FROM caixa_extrato e
       LEFT JOIN caixa_bancos b ON b.id = e.banco_id
       WHERE e.empresa=$1 AND e.ano=$2 AND e.mes=$3 AND e.belvo_tx_id IS NULL
         AND ($4::int IS NULL OR e.banco_id = $4)

       UNION ALL

       SELECT eof.id::text AS id, 
              EXTRACT(DAY FROM eof.data_lancamento)::int AS dia, 
              eof.descricao, 
              eof.razao_social, 
              eof.agencia_numero AS account_number,
              eof.cnpj_cpf AS counterparty_document, 
              ROUND(eof.valor * 100)::int AS valor,
              eof.id::text AS belvo_tx_id, 
              NULL::int AS banco_id, 
              eof.banco AS banco_nome,
              eof.data_lancamento AS atualizado_em
       FROM extrato_openfinance eof
       LEFT JOIN caixa_bancos b ON LOWER(b.nome) = LOWER(eof.banco) AND b.empresa = $1
       WHERE eof.cliente=$1 AND EXTRACT(YEAR FROM eof.data_lancamento)=$2 AND EXTRACT(MONTH FROM eof.data_lancamento)=$3
         AND ($4::int IS NULL OR b.id = $4)
       
       ORDER BY dia, id`,
      ['Lanzi', 2026, 5, null]
    );
    console.log(`Rows: ${res.rows.length}`);
  } catch (err) {
    console.error('Erro na query:', err.message);
  } finally {
    pool.end();
  }
}

run();
