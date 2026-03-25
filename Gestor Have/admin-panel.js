/**
 * Admin Panel Functions
 * Gerenciamento de usuários para admins
 */

let adminUsers = [];
let adminFormMode = 'create'; // 'create' ou 'edit'
let editingUserId = null;

/**
 * Load admin users list
 */
async function loadAdminUsuarios() {
    try {
        setDbStatus('Carregando usuários...', false);
        const token = localStorage.getItem('have_token');
        const res = await fetch(`${API_BASE}/admin/usuarios`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            }
        });

        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        adminUsers = await res.json();
        renderAdminTable();
        setDbStatus('Online', true);
    } catch (e) {
        console.error('loadAdminUsuarios failed:', e);
        setDbStatus('Erro ao carregar', false);
        showToast('Erro ao carregar usuários: ' + e.message, 'error');
    }
}

/**
 * Render admin users table
 */
function renderAdminTable() {
    const tbody = document.getElementById('admin-usuarios-tbody');
    if (!tbody) return;

    if (adminUsers.length === 0) {
        tbody.innerHTML = `<tr><td colspan="6"><div class="empty-state"><div class="empty-icon">👥</div><div class="empty-title">Nenhum usuário encontrado</div></div></td></tr>`;
        return;
    }

    tbody.innerHTML = adminUsers.map(u => `<tr>
        <td><strong>${esc(u.nome)}</strong></td>
        <td><code>${esc(u.usuario)}</code></td>
        <td><span class="badge-role badge-${u.perfil}">${u.perfil}</span></td>
        <td>${u.ativo ? '<span style="color:var(--green)">✓ Ativo</span>' : '<span style="color:var(--muted)">Inativo</span>'}</td>
        <td><small>${new Date(u.criado_em).toLocaleDateString('pt-BR')}</small></td>
        <td>
            <button class="btn-small btn-edit" onclick="openEditUsuario(${u.id})">✎</button>
            <button class="btn-small btn-delete" onclick="openDeleteConfirm(${u.id}, '${esc(u.usuario)}')" ${!u.ativo ? 'disabled' : ''}>🗑</button>
        </td>
    </tr>`).join('');
}

/**
 * Open create user form
 */
function openCreateUsuario() {
    adminFormMode = 'create';
    editingUserId = null;
    document.getElementById('admin-form-title').textContent = 'Novo Usuário';
    document.getElementById('admin-form-nome').value = '';
    document.getElementById('admin-form-usuario').value = '';
    document.getElementById('admin-form-password').value = '';
    document.getElementById('admin-form-password-confirm').value = '';
    document.getElementById('admin-form-perfil').value = 'gestor';
    document.getElementById('admin-form-empresa').value = currentCompany;
    document.getElementById('admin-form-senha-row').style.display = 'block';
    document.getElementById('admin-modal').style.display = 'block';
}

/**
 * Open edit user form
 */
function openEditUsuario(userId) {
    const user = adminUsers.find(u => u.id === userId);
    if (!user) return;

    adminFormMode = 'edit';
    editingUserId = userId;
    document.getElementById('admin-form-title').textContent = `Editar: ${user.usuario}`;
    document.getElementById('admin-form-nome').value = user.nome;
    document.getElementById('admin-form-usuario').value = user.usuario;
    document.getElementById('admin-form-usuario').disabled = true;
    document.getElementById('admin-form-password').value = '';
    document.getElementById('admin-form-password-confirm').value = '';
    document.getElementById('admin-form-perfil').value = user.perfil;
    document.getElementById('admin-form-ativo').checked = user.ativo;
    document.getElementById('admin-form-senha-row').style.display = 'block';
    document.getElementById('admin-form-ativo-row').style.display = 'block';
    document.getElementById('admin-modal').style.display = 'block';
}

/**
 * Close modal
 */
function closeAdminModal() {
    document.getElementById('admin-modal').style.display = 'none';
    document.getElementById('admin-form-usuario').disabled = false;
    document.getElementById('admin-form-ativo-row').style.display = 'none';
}

/**
 * Open delete confirmation
 */
function openDeleteConfirm(userId, username) {
    if (!confirm(`Desativar usuário "${username}"? Esta ação não pode ser desfeita.`)) return;
    deleteAdminUsuario(userId);
}

/**
 * Submit admin form
 */
async function submitAdminForm() {
    const nome = document.getElementById('admin-form-nome').value.trim();
    const usuario = document.getElementById('admin-form-usuario').value.trim();
    const password = document.getElementById('admin-form-password').value;
    const passwordConfirm = document.getElementById('admin-form-password-confirm').value;
    const perfil = document.getElementById('admin-form-perfil').value;
    const empresa = document.getElementById('admin-form-empresa').value;
    const ativo = document.getElementById('admin-form-ativo')?.checked;

    // Validation
    if (!nome || !usuario || !perfil) {
        showToast('Preencha todos os campos obrigatórios', 'error');
        return;
    }

    if (adminFormMode === 'create') {
        if (!password || password.length < 8) {
            showToast('Senha deve ter no mínimo 8 caracteres', 'error');
            return;
        }
        if (password !== passwordConfirm) {
            showToast('As senhas não conferem', 'error');
            return;
        }
    } else {
        // Edit mode: senha é opcional
        if (password && password.length < 8) {
            showToast('Senha deve ter no mínimo 8 caracteres', 'error');
            return;
        }
        if (password && password !== passwordConfirm) {
            showToast('As senhas não conferem', 'error');
            return;
        }
    }

    try {
        const token = localStorage.getItem('have_token');
        const headers = {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
        };

        if (adminFormMode === 'create') {
            // Create
            const res = await fetch(`${API_BASE}/admin/usuarios`, {
                method: 'POST',
                headers,
                body: JSON.stringify({ nome, usuario, password, perfil, empresa })
            });

            if (!res.ok) {
                const err = await res.json();
                throw new Error(err.error || 'Erro ao criar usuário');
            }

            showToast('Usuário criado com sucesso', 'success');
            closeAdminModal();
            await loadAdminUsuarios();
        } else {
            // Edit
            const updates = { nome, perfil };
            if (ativo !== undefined) updates.ativo = ativo;
            if (password) updates.senha_hash = true; // Flag to indicate password change

            const res = await fetch(`${API_BASE}/admin/usuarios/${editingUserId}`, {
                method: 'PATCH',
                headers,
                body: JSON.stringify(updates)
            });

            if (!res.ok) {
                const err = await res.json();
                throw new Error(err.error || 'Erro ao atualizar usuário');
            }

            // If password changed, send reset
            if (password) {
                const resetRes = await fetch(`${API_BASE}/admin/usuarios/${editingUserId}/reset-password`, {
                    method: 'PUT',
                    headers,
                    body: JSON.stringify({ tempPassword: password })
                });

                if (!resetRes.ok) {
                    throw new Error('Erro ao atualizar senha');
                }
            }

            showToast('Usuário atualizado com sucesso', 'success');
            closeAdminModal();
            await loadAdminUsuarios();
        }
    } catch (e) {
        console.error('submitAdminForm failed:', e);
        showToast('Erro: ' + e.message, 'error');
    }
}

/**
 * Delete (deactivate) admin user
 */
async function deleteAdminUsuario(userId) {
    try {
        const token = localStorage.getItem('have_token');
        const res = await fetch(`${API_BASE}/admin/usuarios/${userId}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            }
        });

        if (!res.ok) {
            const err = await res.json();
            throw new Error(err.error || 'Erro ao desativar usuário');
        }

        showToast('Usuário desativado com sucesso', 'success');
        await loadAdminUsuarios();
    } catch (e) {
        console.error('deleteAdminUsuario failed:', e);
        showToast('Erro: ' + e.message, 'error');
    }
}

// Styles for admin panel
const adminStyles = `
#admin-section { display: none; }
#admin-nav { display: none; }

.badge-role {
    padding: 4px 8px;
    border-radius: 6px;
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
}

.badge-admin { background: var(--accent); color: #fff; }
.badge-gestor { background: var(--orange); color: #fff; }
.badge-have { background: var(--accent2); color: #fff; }

.btn-small {
    padding: 6px 10px;
    border: none;
    border-radius: 6px;
    background: var(--surface2);
    color: var(--text);
    cursor: pointer;
    margin-right: 4px;
    font-size: 14px;
    transition: all 0.2s;
}

.btn-small:hover { background: var(--accent); color: #fff; }
.btn-delete:hover { background: var(--red); }
.btn-small:disabled { opacity: 0.5; cursor: not-allowed; }

#admin-modal {
    display: none;
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background: rgba(0, 0, 0, 0.7);
    z-index: 1000;
    align-items: center;
    justify-content: center;
    flex-direction: column;
}

#admin-modal.active {
    display: flex;
}

.admin-modal-content {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 24px;
    max-width: 400px;
    width: 90%;
    max-height: 90vh;
    overflow-y: auto;
}

.admin-form-group {
    margin-bottom: 16px;
}

.admin-form-group label {
    display: block;
    font-size: 12px;
    color: var(--muted);
    margin-bottom: 6px;
    font-weight: 500;
}

.admin-form-group input,
.admin-form-group select {
    width: 100%;
    padding: 10px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--surface2);
    color: var(--text);
    font-family: inherit;
}

.admin-form-group input:focus,
.admin-form-group select:focus {
    outline: none;
    border-color: var(--accent);
    box-shadow: 0 0 0 3px rgba(79, 127, 255, 0.1);
}

.admin-modal-actions {
    display: flex;
    gap: 10px;
    margin-top: 24px;
}

.admin-modal-actions button {
    flex: 1;
    padding: 10px;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-weight: 600;
}

.btn-submit {
    background: var(--accent);
    color: #fff;
}

.btn-submit:hover {
    background: #3d68ff;
}

.btn-cancel {
    background: var(--surface2);
    color: var(--text);
}

.btn-cancel:hover {
    background: var(--border);
}
`;

// Add styles to document
const styleEl = document.createElement('style');
styleEl.textContent = adminStyles;
document.head.appendChild(styleEl);

// Intercept showPanel to load admin data when admin panel is shown
const originalShowPanel = window.showPanel;
window.showPanel = function(id) {
    originalShowPanel(id);
    if (id === 'admin-usuarios' && adminUsers.length === 0) {
        loadAdminUsuarios();
    }
};

// Load admin data on page load if user is admin (after JWT is decoded)
document.addEventListener('DOMContentLoaded', async function() {
    // Wait a bit for JWT to be decoded in startDashboard
    setTimeout(() => {
        const adminNav = document.getElementById('admin-nav');
        if (adminNav && adminNav.style.display === 'flex') {
            // User is admin, preload data
            loadAdminUsuarios();
        }
    }, 1000);
});
