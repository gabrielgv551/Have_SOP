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
  supershop: {
    name: "Supershop",
    dbEnvKey: "SUPERSHOP",
    users: {
      admin:  process.env.SUPERSHOP_PASS_ADMIN,
      gestor: process.env.SUPERSHOP_PASS_GESTOR,
      have:   process.env.SUPERSHOP_PASS_HAVE,
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
  shopgra: {
    name: "Shopgra",
    dbEnvKey: "SHOPGRA",
    users: {
      admin:  process.env.SHOPGRA_PASS_ADMIN,
      gestor: process.env.SHOPGRA_PASS_GESTOR,
      have:   process.env.SHOPGRA_PASS_HAVE,
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
