// ── Helpers de LLM compartilhados ────────────────────────────────────────────
// Providers: Anthropic (Claude), Groq (Llama), Gemini
// Usado por: api/agents.js (Anthropic) e api/cenarios.js (Groq + Gemini)

// ─── Anthropic ────────────────────────────────────────────────────────────────

const SYSTEM_FOOTER = `

REGRAS ABSOLUTAS DE INTERPRETAÇÃO DE VALORES:
- Todos os valores numéricos estão em REAIS (R$) exatos. O que está no dado É o valor real.
- 850000 = R$ 850.000,00 (oitocentos e cinquenta MIL reais) — NÃO é milhão.
- 1500000 = R$ 1.500.000,00 (um milhão e meio) — NÃO é 1,5 bilhão.
- NUNCA multiplique nem divida os valores recebidos.
- Sempre formate usando padrão brasileiro: ponto para milhar, vírgula para decimal.
- Exemplo correto: R$ 850.000,00 | R$ 1.234.567,89`;

async function anthropicRequest(body) {
  const maxRetries = 3;
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify(body),
    });
    if (response.status === 429) {
      const wait = (attempt + 1) * 20000;
      console.warn(`[anthropicRequest] 429 rate limit, aguardando ${wait/1000}s (tentativa ${attempt+1}/${maxRetries})`);
      await new Promise(r => setTimeout(r, wait));
      continue;
    }
    if (!response.ok) {
      const errText = await response.text();
      throw new Error(`Anthropic API error ${response.status}: ${errText}`);
    }
    return response.json();
  }
  throw new Error('Anthropic API: limite de tentativas esgotado após rate limit 429');
}

// ─── Groq (Llama 3.3 70B — principal para cenários) ──────────────────────────

async function callGroq(messages, maxTokens = 1200) {
  const key = (process.env.GROQ_API_KEY || '').trim();
  if (!key) throw new Error('GROQ_API_KEY não configurada');
  const models = ['llama-3.3-70b-versatile', 'llama-3.1-8b-instant'];
  let lastErr;
  for (const model of models) {
    const r = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${key}` },
      body: JSON.stringify({ model, max_tokens: maxTokens, temperature: 0.3, messages }),
    });
    if (r.ok) {
      const d = await r.json();
      return (d.choices[0].message.content || '').trim();
    }
    const errBody = await r.text();
    lastErr = `Groq ${r.status} (${model}): ${errBody.slice(0, 200)}`;
    if (r.status !== 429) break;
  }
  throw new Error(lastErr);
}

// ─── Gemini (fallback para cenários) ─────────────────────────────────────────

async function callGemini(messages, maxTokens = 1200) {
  const key = (process.env.GEMINI_API_KEY || '').trim();
  if (!key) throw new Error('GEMINI_API_KEY não configurada');
  const sysMsg = messages.find(m => m.role === 'system');
  const chatMsgs = messages.filter(m => m.role !== 'system');
  const body = {
    contents: chatMsgs.map(m => ({
      role: m.role === 'assistant' ? 'model' : 'user',
      parts: [{ text: m.content }],
    })),
    generationConfig: { maxOutputTokens: maxTokens, temperature: 0.3 },
  };
  if (sysMsg) body.systemInstruction = { parts: [{ text: sysMsg.content }] };
  const r = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${key}`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }
  );
  if (!r.ok) { const e = await r.text(); throw new Error(`Gemini ${r.status}: ${e.slice(0, 200)}`); }
  const d = await r.json();
  return (d.candidates?.[0]?.content?.parts?.[0]?.text || '').trim();
}

// ─── Fallback automático: Groq → Gemini ──────────────────────────────────────

async function callAI(messages, maxTokens = 1200) {
  if ((process.env.GROQ_API_KEY || '').trim()) {
    try { return await callGroq(messages, maxTokens); } catch (e) {
      console.warn('[callAI] Groq falhou, tentando Gemini:', e.message);
    }
  }
  return callGemini(messages, maxTokens);
}

module.exports = { SYSTEM_FOOTER, anthropicRequest, callGroq, callGemini, callAI };
