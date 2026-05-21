#!/usr/bin/env python3
"""
Reads and decrypts Chrome cookies for erp.olist.com / tiny.com.br.
Uses built-in sqlite3 + cryptography + PowerShell DPAPI (no win32crypt needed).
Outputs JSON with {name: value} pairs.

Usage:
  python read_chrome_cookies.py
  python read_chrome_cookies.py --host erp.olist.com
"""
import os, sys, json, sqlite3, shutil, tempfile, base64, subprocess

def get_chrome_key():
    local_state_path = os.path.join(
        os.environ.get('LOCALAPPDATA', ''),
        'Google', 'Chrome', 'User Data', 'Local State'
    )
    with open(local_state_path, 'r', encoding='utf-8') as f:
        ls = json.load(f)
    enc_key_b64 = ls['os_crypt']['encrypted_key']
    enc_key = base64.b64decode(enc_key_b64)[5:]  # Remove 'DPAPI' prefix
    # Decrypt via PowerShell (no win32crypt dependency)
    ps_cmd = (
        '[Convert]::ToBase64String('
        '[System.Security.Cryptography.ProtectedData]::Unprotect('
        f'[Convert]::FromBase64String("{base64.b64encode(enc_key).decode()}"),'
        '$null, "CurrentUser"))'
    )
    result = subprocess.run(
        ['powershell', '-NoProfile', '-NonInteractive', '-Command', ps_cmd],
        capture_output=True, text=True
    )
    return base64.b64decode(result.stdout.strip())

def decrypt_cookie(key, enc_value):
    if not enc_value or len(enc_value) < 16:
        return ''
    try:
        prefix = enc_value[:3]
        if prefix in (b'v10', b'v11', b'v20'):
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            nonce = enc_value[3:15]
            data  = enc_value[15:]
            aesgcm = AESGCM(key)
            return aesgcm.decrypt(nonce, data, None).decode('utf-8')
    except Exception:
        pass
    return ''

def main():
    host_filter = sys.argv[sys.argv.index('--host') + 1] if '--host' in sys.argv else None
    cookies_src = os.path.join(
        os.environ.get('LOCALAPPDATA', ''),
        'Google', 'Chrome', 'User Data', 'Default', 'Network', 'Cookies'
    )
    if not os.path.exists(cookies_src):
        cookies_src = os.path.join(
            os.environ.get('LOCALAPPDATA', ''),
            'Google', 'Chrome', 'User Data', 'Default', 'Cookies'
        )

    # Try to copy first; fall back to reading directly via nolock URI
    tmp = os.path.join(tempfile.gettempdir(), 'chrome_cookies_olist.db')
    copied = False
    try:
        shutil.copy2(cookies_src, tmp)
        if os.path.getsize(tmp) > 0:
            copied = True
    except Exception:
        pass

    db_path = tmp if copied else cookies_src

    try:
        key = get_chrome_key()
    except Exception as e:
        print(json.dumps({'error': f'Failed to get Chrome key: {e}'}), file=sys.stderr)
        sys.exit(1)

    # Use nolock=1 so we can read even when Chrome has the file open
    uri = f'file:{db_path.replace(chr(92), "/")}?mode=ro&nolock=1'
    try:
        conn = sqlite3.connect(uri, uri=True)
    except Exception:
        conn = sqlite3.connect(db_path)  # last resort
    cursor = conn.cursor()
    if host_filter:
        cursor.execute(
            "SELECT host_key, name, encrypted_value FROM cookies WHERE host_key LIKE ?",
            (f'%{host_filter}%',)
        )
    else:
        cursor.execute(
            "SELECT host_key, name, encrypted_value FROM cookies "
            "WHERE host_key LIKE '%olist.com%' OR host_key LIKE '%tiny.com.br%' "
            "OR host_key LIKE '%accounts.tiny%'"
        )
    rows = cursor.fetchall()
    conn.close()

    result = {}
    cookie_str_parts = []
    for host_key, name, enc_value in rows:
        val = decrypt_cookie(key, bytes(enc_value) if enc_value else b'')
        if val:
            result[name] = val
            cookie_str_parts.append(f'{name}={val}')

    output = {
        'cookies': result,
        'cookie_string': '; '.join(cookie_str_parts),
        'count': len(result),
    }
    print(json.dumps(output))

if __name__ == '__main__':
    main()
