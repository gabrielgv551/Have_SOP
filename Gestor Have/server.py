from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import psycopg2
import os

DB_CONFIG = {
    "host": "37.60.236.200",
    "port": 5432,
    "database": "Lanzi",
    "user": "postgres",
    "password": "131105Gv",
}

class Handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        print(f"[{self.address_string()}] {format % args}")

    def send_cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_cors()
        self.end_headers()

    def do_POST(self):
        if self.path != "/query":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        try:
            payload = json.loads(body)
            sql = payload.get("sql", "")

            if not sql:
                raise ValueError("SQL vazio")

            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            cur.close()
            conn.close()

            response = json.dumps(rows, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_cors()
            self.end_headers()
            self.wfile.write(response)

        except Exception as e:
            error = json.dumps({"error": str(e)}).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_cors()
            self.end_headers()
            self.wfile.write(error)
            print(f"[ERRO] {e}")

if __name__ == "__main__":
    port = 8787
    print(f"✅ Servidor Have rodando em http://localhost:{port}")
    print(f"   Conectado ao banco: {DB_CONFIG['database']} em {DB_CONFIG['host']}")
    print(f"   Pressione Ctrl+C para parar.\n")
    HTTPServer(("localhost", port), Handler).serve_forever()
