const jwt = require('jsonwebtoken');
const token = jwt.sign({ company: 'Lanzi' }, 'have2025secure');

fetch("https://have-gestor-api.vercel.app/api/caixa-extrato?ano=2026&mes=5", {
  headers: { 'Authorization': 'Bearer ' + token }
})
.then(r => r.json())
.then(d => {
  if (d.error) console.error("Error:", d.error);
  else console.log("Rows count:", d.rows ? d.rows.length : "NO ROWS");
})
.catch(e => console.error("Crash:", e));
