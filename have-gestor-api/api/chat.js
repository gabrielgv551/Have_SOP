const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const companies = require('../lib/companies');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') return res.status(200).end();

  // 1. Validar Token
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) return res.status(401).json({ error: 'Não autorizado' });

  let payload;
  try {
    payload = jwt.verify(auth, process.env.JWT_SECRET);
  } catch {
    return res.status(401).json({ error: 'Token inválido' });
  }

  const { message, contextData } = req.body;

  // 2. Chamar a API do Claude (Anthropic)
  try {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model: "claude-3-5-sonnet-20241022",
        max_tokens: 1024,
        system: `Você é o assistente inteligente do sistema "Have · Gestor Inteligente". 
        Você analisa dados de S&OP (Estoque, Curva ABC, Ponto de Pedido).
        Empresa atual: ${payload.companyName}.
        Seja direto, profissional e ajude o gestor a tomar decisões de compra.`,
        messages: [
          {
            role: "user",
            content: `Pergunta do Gestor: ${message}\n\nContexto dos Dados:\n${JSON.stringify(contextData)}`
          }
        ]
      })
    });

    const data = await response.json();
    res.json({ reply: data.content[0].text });

  } catch (error) {
    console.error('Erro no Claude:', error);
    res.status(500).json({ error: 'Falha ao processar análise do Claude' });
  }
};
