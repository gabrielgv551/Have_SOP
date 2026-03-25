"""
╔══════════════════════════════════════════════════════════════╗
║         Have · Gestor Inteligente · Backend Flask            ║
║  Serve o index.html e faz proxy das queries ao PostgreSQL    ║
╚══════════════════════════════════════════════════════════════╝

Como rodar no servidor (37.60.236.200):
  pip install flask psycopg2-binary
  python app.py

Acesso: http://37.60.236.200:8080
"""

from flask import Flask, request, jsonify, send_from_directory
import psycopg2
import os

app = Flask(__name__, static_folder=".")

DB_CONFIG = {
    "host":     "localhost",   # roda no próprio servidor, usa localhost
    "port":     5432,
    "database": "Lanzi",
    "user":     "postgres",
    "password": "131105Gv",
}

# ── Servir o index.html ──────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(".", "index.html")

# ── API de queries ───────────────────────────────────────────
@app.route("/query", methods=["POST", "OPTIONS"])
def query():
    if request.method == "OPTIONS":
        resp = app.make_default_options_response()
        resp.headers["Access-Control-Allow-Origin"]  = "*"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return resp

    payload = request.get_json(silent=True) or {}
    sql     = payload.get("sql", "").strip()

    if not sql:
        return jsonify({"error": "SQL vazio"}), 400

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()

        resp = jsonify(rows)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    except Exception as e:
        print(f"[ERRO SQL] {e}")
        resp = jsonify({"error": str(e)})
        resp.status_code = 500
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp


if __name__ == "__main__":
    porta = int(os.environ.get("PORT", 8080))
    print(f"✅ Gestor Have rodando em http://0.0.0.0:{porta}")
    print(f"   Banco: {DB_CONFIG['database']} ({DB_CONFIG['host']})")
    app.run(host="0.0.0.0", port=porta, debug=False)
