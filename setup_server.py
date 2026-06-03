"""
Setup script para instalar dependências no servidor
Rode uma vez no servidor antes de executar o extrator
"""
import subprocess
import sys

def run(cmd):
    print(f">>> {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"ERRO: {result.stderr}")
    return result.returncode == 0

print("=" * 50)
print("INSTALANDO DEPENDENCIAS NO SERVIDOR")
print("=" * 50)

# Instala pip se necessario
run("apt-get update -qq")
run("apt-get install -y -qq python3-pip")

# Instala dependencias Python
run(f"{sys.executable} -m pip install playwright requests urllib3 --break-system-packages")

# Instala browsers do Playwright
run("python3 -m playwright install chromium")
run("python3 -m playwright install-deps chromium")

print("\n" + "=" * 50)
print("SETUP CONCLUIDO!")
print("=" * 50)
