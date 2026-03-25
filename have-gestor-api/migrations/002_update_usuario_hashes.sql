-- Update bcrypt password hashes for initial users
-- These are the hashes for: admin/lanzi2024, gestor/have2024, have/lanzi@2024

UPDATE usuarios SET senha_hash = '$2b$10$lCc19AJoyYFo16.gILWg/.iM1vnRMN6bjz6drL9L65dzDCOR4Bmny' WHERE usuario = 'admin' AND empresa = 'lanzi';
UPDATE usuarios SET senha_hash = '$2b$10$KZcrbinKQZBwTdPJPS197Oo9ss5bvQMjNgaizB96LbSuSIIxY6SPa' WHERE usuario = 'gestor' AND empresa = 'lanzi';
UPDATE usuarios SET senha_hash = '$2b$10$T16uI1FTVN3YuSvFTN1EV.N1vDWf2kf4xuss57.RiuFxioPF/gN76' WHERE usuario = 'have' AND empresa = 'lanzi';

-- Verify hashes were updated
SELECT usuario, perfil, (LENGTH(senha_hash) > 0) as has_hash FROM usuarios WHERE empresa = 'lanzi' ORDER BY usuario;
