// Configuração das empresas
// Senhas do banco e dos usuários ficam nas variáveis de ambiente do Vercel
// NUNCA colocar credenciais direto aqui

module.exports = {
  lanzi: {
    name: "Lanzi",
    dbEnvKey: "LANZI",
    users: {
      admin:  process.env.LANZI_PASS_ADMIN,
      gestor: process.env.LANZI_PASS_GESTOR,
      have:   process.env.LANZI_PASS_HAVE,
    }
  },
  marcon: {
    name: "Marcon",
    dbEnvKey: "MARCON",
    users: {
      admin:  process.env.MARCON_PASS_ADMIN,
      gestor: process.env.MARCON_PASS_GESTOR,
      have:   process.env.MARCON_PASS_HAVE,
    }
  },
  // Para adicionar nova empresa, basta copiar o bloco acima e
  // adicionar as variáveis de ambiente correspondentes no Vercel.
  // empresa2: {
  //   name: "Empresa 2",
  //   dbEnvKey: "EMP2",
  //   users: { admin: process.env.EMP2_PASS_ADMIN }
  // }
};
