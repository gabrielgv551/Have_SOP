import { Pool } from 'pg';
import bcrypt from 'bcryptjs';
import { protect } from '../../lib/auth';

const pool = new Pool({
    connectionString: `postgres://${process.env.LANZI_USER}:${process.env.LANZI_PASSWORD}@${process.env.LANZI_HOST}:${process.env.LANZI_PORT}/${process.env.LANZI_DB}`,
});

async function handler(req, res) {
    // --- Listar Usuários (GET) ---
    if (req.method === 'GET') {
        try {
            const { rows } = await pool.query('SELECT id, nome, email, role, ativo, data_criacao FROM usuarios ORDER BY nome');
            return res.status(200).json(rows);
        } catch (error) {
            return res.status(500).json({ error: 'Erro ao buscar usuários.', details: error.message });
        }
    }

    // --- Criar Novo Usuário (POST) ---
    if (req.method === 'POST') {
        const { nome, email, senha, role } = req.body;

        if (!nome || !email || !senha || !role) {
            return res.status(400).json({ error: 'Todos os campos são obrigatórios: nome, email, senha, role.' });
        }

        try {
            // Criptografa a senha antes de salvar
            const senha_hash = await bcrypt.hash(senha, 10);

            const { rows } = await pool.query(
                'INSERT INTO usuarios (nome, email, senha_hash, role) VALUES ($1, $2, $3, $4) RETURNING id, nome, email, role, ativo',
                [nome, email, senha_hash, role]
            );

            return res.status(201).json(rows[0]);
        } catch (error) {
            if (error.code === '23505') { // Código de violação de unicidade (email duplicado)
                return res.status(409).json({ error: 'Este email já está em uso.' });
            }
            return res.status(500).json({ error: 'Erro ao criar usuário.', details: error.message });
        }
    }

    // --- Método não permitido ---
    res.setHeader('Allow', ['GET', 'POST']);
    return res.status(405).end(`Método ${req.method} não permitido.`);
}

// Protege a rota: apenas usuários com o papel 'admin' podem acessar.
export default protect(['admin'])(handler);