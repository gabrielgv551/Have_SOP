"""
Mercado Livre — Seed de dados de teste
=======================================
Gera pedidos fictícios no formato exato da API do ML e salva na tabela
bd_vendas_ml para fins de desenvolvimento e validação do schema.

Os pedidos são gerados com todos os campos possíveis preenchidos,
incluindo detalhes de envio (sem chamar a API real).

USO:
  python -m mercadolivre.seed_test          # gera 100 pedidos fake
  N=50 python -m mercadolivre.seed_test     # gera 50 pedidos
  TRUNCATE=1 python -m mercadolivre.seed_test  # limpa a tabela antes de inserir
"""

import os
import sys
import random
import string
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

from mercadolivre.etl import criar_tabela_se_necessario, expandir_pedido, upsert_banco
from db import get_engine

N_PEDIDOS = int(os.getenv("N", 100))
TRUNCATE  = os.getenv("TRUNCATE", "0") == "1"

# ─── Semente fixa para reprodutibilidade ─────────────────────
random.seed(42)

# ─── Catálogo de produtos ─────────────────────────────────────
PRODUTOS = [
    {"id": "MLB3001", "title": "Notebook Dell Inspiron 15 8GB 256GB SSD",          "sku": "NB-DELL-001",   "cat": "MLB1648", "price": 2899.90, "peso": 2.1,  "listing": "gold_special"},
    {"id": "MLB3002", "title": "Teclado Mecânico Gamer RGB Switch Blue ABNT2",      "sku": "TEC-MEC-001",   "cat": "MLB1714", "price": 349.90,  "peso": 0.8,  "listing": "gold_pro"},
    {"id": "MLB3003", "title": "Monitor LG 24\" Full HD IPS 75Hz",                  "sku": "MON-LG-001",    "cat": "MLB1027", "price": 1199.90, "peso": 3.5,  "listing": "gold_special"},
    {"id": "MLB3004", "title": "Mouse Gamer Logitech G502 HERO 25600 DPI",          "sku": "MOU-LOG-001",   "cat": "MLB1714", "price": 399.90,  "peso": 0.3,  "listing": "gold_pro"},
    {"id": "MLB3005", "title": "Headset Bluetooth Sony WH-1000XM5 Cancelamento",    "sku": "HEA-SON-001",   "cat": "MLB1736", "price": 1799.90, "peso": 0.25, "listing": "gold_special"},
    {"id": "MLB3006", "title": "Cadeira Gamer ThunderX3 EC3 Preto/Vermelho",        "sku": "CAD-TX3-001",   "cat": "MLB1574", "price": 1499.90, "peso": 18.0, "listing": "gold_special"},
    {"id": "MLB3007", "title": "Cabo HDMI 2.0 4K 60Hz 3 Metros Flexível",          "sku": "CAB-HDMI-001",  "cat": "MLB1034", "price": 39.90,   "peso": 0.15, "listing": "gold_pro"},
    {"id": "MLB3008", "title": "SSD Kingston 480GB SATA III 2.5\" A400",            "sku": "SSD-KING-001",  "cat": "MLB1652", "price": 279.90,  "peso": 0.08, "listing": "gold_special"},
    {"id": "MLB3009", "title": "Memória RAM DDR4 16GB 3200MHz Kingston Fury",       "sku": "RAM-KING-001",  "cat": "MLB1652", "price": 349.90,  "peso": 0.05, "listing": "gold_pro"},
    {"id": "MLB3010", "title": "Fonte ATX 650W 80 Plus Bronze Corsair CV650",       "sku": "FON-COR-001",   "cat": "MLB1652", "price": 499.90,  "peso": 1.8,  "listing": "gold_special"},
    {"id": "MLB3011", "title": "Placa de Vídeo RX 6600 8GB GDDR6 Dual Fan",        "sku": "GPU-RX6600-001","cat": "MLB1652", "price": 1899.90, "peso": 0.9,  "listing": "gold_special"},
    {"id": "MLB3012", "title": "Roteador Wi-Fi 6 TP-Link Archer AX3000 Dual Band", "sku": "ROU-TP-001",    "cat": "MLB1801", "price": 699.90,  "peso": 0.7,  "listing": "gold_pro"},
    {"id": "MLB3013", "title": "Impressora Multifuncional HP DeskJet 2874",         "sku": "IMP-HP-001",    "cat": "MLB1700", "price": 599.90,  "peso": 4.1,  "listing": "gold_special"},
    {"id": "MLB3014", "title": "Webcam Full HD 1080p 30fps Logitech C920",          "sku": "CAM-LOG-001",   "cat": "MLB1736", "price": 599.90,  "peso": 0.16, "listing": "gold_pro"},
    {"id": "MLB3015", "title": "Hub USB-C 7 em 1 Thunderbolt 4K HDMI",             "sku": "HUB-USB-001",   "cat": "MLB1034", "price": 199.90,  "peso": 0.12, "listing": "gold_pro"},
    {"id": "MLB3016", "title": "Cooler CPU Noctua NH-D15 Dual Tower 140mm",        "sku": "COO-NOC-001",   "cat": "MLB1652", "price": 799.90,  "peso": 1.32, "listing": "gold_special"},
    {"id": "MLB3017", "title": "Gabinete ATX Vidro Temperado RGB Corsair 4000D",   "sku": "GAB-COR-001",   "cat": "MLB1652", "price": 899.90,  "peso": 6.8,  "listing": "gold_special"},
    {"id": "MLB3018", "title": "Pendrive USB 3.0 128GB Kingston DataTraveler",      "sku": "PEN-KING-001",  "cat": "MLB1034", "price": 79.90,   "peso": 0.02, "listing": "gold_pro"},
    {"id": "MLB3019", "title": "Nobreak APC 700VA 115V Bivolt Smart-UPS",          "sku": "NOB-APC-001",   "cat": "MLB1801", "price": 699.90,  "peso": 8.3,  "listing": "gold_special"},
    {"id": "MLB3020", "title": "Mousepad Gamer XL 900x400mm Antiderrapante",       "sku": "PAD-GAM-001",   "cat": "MLB1714", "price": 89.90,   "peso": 0.5,  "listing": "gold_pro"},
]

COMPRADORES = [
    {"id": 201001, "nick": "JOAO_SILVA_SP",    "email": "joao.silva@gmail.com",       "fn": "João",      "ln": "Silva",      "tel": "11987654321"},
    {"id": 201002, "nick": "MARIA_SANTOS_RJ",  "email": "maria.santos@hotmail.com",   "fn": "Maria",     "ln": "Santos",     "tel": "21976543210"},
    {"id": 201003, "nick": "CARLOS_MG",        "email": "carlos.oliveira@yahoo.com.br","fn": "Carlos",   "ln": "Oliveira",   "tel": "31965432109"},
    {"id": 201004, "nick": "ANA_PAULA_PR",     "email": "ana.paula@gmail.com",        "fn": "Ana Paula", "ln": "Ferreira",   "tel": "41954321098"},
    {"id": 201005, "nick": "PEDRO_COSTA_RS",   "email": "pedro.costa@outlook.com",    "fn": "Pedro",     "ln": "Costa",      "tel": "51943210987"},
    {"id": 201006, "nick": "JULIA_LIMA_BA",    "email": "julia.lima@gmail.com",       "fn": "Júlia",     "ln": "Lima",       "tel": "71932109876"},
    {"id": 201007, "nick": "RAFAEL_SOUZA_CE",  "email": "rafael.souza@gmail.com",     "fn": "Rafael",    "ln": "Souza",      "tel": "85921098765"},
    {"id": 201008, "nick": "CAMILA_ROCHA_SC",  "email": "camila.rocha@icloud.com",    "fn": "Camila",    "ln": "Rocha",      "tel": "48910987654"},
    {"id": 201009, "nick": "LUCAS_ALVES_GO",   "email": "lucas.alves@gmail.com",      "fn": "Lucas",     "ln": "Alves",      "tel": "62900876543"},
    {"id": 201010, "nick": "FERNANDA_PE",      "email": "fernanda.melo@gmail.com",    "fn": "Fernanda",  "ln": "Melo",       "tel": "81998765432"},
]

CIDADES_ESTADOS = [
    ("São Paulo", "SP"),    ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"),
    ("Curitiba", "PR"),     ("Porto Alegre", "RS"),   ("Salvador", "BA"),
    ("Florianópolis", "SC"),("Goiânia", "GO"),        ("Fortaleza", "CE"),
    ("Recife", "PE"),       ("Manaus", "AM"),          ("Belém", "PA"),
]

METODOS_PAG  = ["credit_card", "account_money", "debit_card", "bolbradesco", "pix"]
PAYMENT_TYPE = ["credit_card", "account_money", "debit_card", "ticket", "bank_transfer"]
OP_TYPES     = ["regular_payment", "buy_in_installments"]
LOG_TYPES    = ["fulfillment", "xd_drop_off", "default_operating_type", "self_service"]
SHIP_MODES   = ["me2", "me1"]
SHIP_STATUS  = ["delivered", "shipped", "handling", "ready_to_ship"]
TRACK_METHODS = ["post_office", "correios", "carrier"]
CANAIS       = ["marketplace", "marketplace", "marketplace", "mshops"]
# distribuição realista: ~85% paid, ~10% cancelled, ~5% outros
STATUSES     = (["paid"] * 85) + (["cancelled"] * 10) + (["payment_required"] * 3) + (["invalid"] * 2)

SELLER_ID   = 3364392047
SELLER_NICK = "LANZI_OFICIAL"

# Data base: últimos 3 meses
_DATA_FIM = datetime.now()
_DATA_INI = _DATA_FIM - timedelta(days=90)


def _rand_str(n=10) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))


def _rand_cpf() -> str:
    return "".join([str(random.randint(0, 9)) for _ in range(11)])


def _rand_cnpj() -> str:
    return "".join([str(random.randint(0, 9)) for _ in range(14)])


def _rand_cep() -> str:
    return f"{random.randint(10000, 99999):05d}-{random.randint(100, 999)}"


def _isodt(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def gerar_pedido_fake(order_id: int, base_date: datetime) -> dict:
    """Gera um pedido no formato exato do objeto retornado pela API do ML."""
    b      = random.choice(COMPRADORES)
    cidade = random.choice(CIDADES_ESTADOS)
    status = random.choice(STATUSES)

    dt_criacao   = base_date
    dt_aprovacao = (dt_criacao + timedelta(minutes=random.randint(2, 90))) if status == "paid" else None
    dt_fechamento = (dt_aprovacao + timedelta(days=random.randint(7, 45))) if dt_aprovacao else None
    dt_last_upd  = dt_fechamento or dt_aprovacao or dt_criacao
    dt_expira    = dt_criacao + timedelta(days=30)

    # ── Itens (1 a 3 produtos por pedido)
    n_itens = random.choices([1, 2, 3], weights=[70, 20, 10])[0]
    produtos_selecionados = random.sample(PRODUTOS, n_itens)

    total_amount = 0.0
    order_items  = []
    for p in produtos_selecionados:
        qty        = random.choices([1, 2, 3], weights=[80, 15, 5])[0]
        price      = round(p["price"] * random.uniform(0.92, 1.08), 2)
        full_price = round(price * random.uniform(1.0, 1.18), 2)
        total_amount += qty * price
        order_items.append({
            "item": {
                "id":                   p["id"],
                "title":                p["title"],
                "category_id":          p["cat"],
                "variation_id":         None,
                "seller_custom_field":  None,
                "global_price":         None,
                "net_weight":           p["peso"],
                "variation_attributes": [],
                "warranty":             "Garantia de fábrica: 12 meses",
                "condition":            "new",
                "seller_sku":           p["sku"],
                "listing_type_id":      p["listing"],
                "thumbnail":            f"https://http2.mlstatic.com/D_{p['id']}-F.jpg",
            },
            "quantity":           qty,
            "requested_quantity": {"value": qty, "measure": "unit"},
            "picked_quantity":    None,
            "unit_price":         price,
            "full_unit_price":    full_price,
            "currency_id":        "BRL",
            "manufacturing_days": None,
            "sale_fee":           round(price * random.uniform(0.11, 0.16), 2),
        })

    total_amount    = round(total_amount, 2)
    ship_amount     = round(random.choices([0.0, random.uniform(14, 48)], weights=[35, 65])[0], 2)
    mkt_fee         = round(total_amount * random.uniform(0.11, 0.16), 2)
    coupon_val      = round(random.choices([0.0, random.uniform(5, 60)], weights=[80, 20])[0], 2)
    taxes_val       = round(total_amount * 0.01, 2)
    paid_amount     = round(total_amount + ship_amount - coupon_val, 2)

    parcelas        = random.choices([1, 2, 3, 6, 10, 12], weights=[30, 15, 15, 20, 10, 10])[0]
    met_pag         = random.choice(METODOS_PAG)
    ptype           = random.choice(PAYMENT_TYPE)
    logistic        = random.choice(LOG_TYPES)
    ship_mode       = random.choice(SHIP_MODES)
    ship_status     = random.choice(SHIP_STATUS) if status == "paid" else "cancelled"
    ship_id         = order_id + 9_000_000_000
    doc_tipo        = random.choice(["CPF", "CNPJ"])
    doc_num         = _rand_cpf() if doc_tipo == "CPF" else _rand_cnpj()

    # ── Datas do envio
    dt_handling  = (dt_aprovacao + timedelta(hours=random.randint(2, 36))) if dt_aprovacao else None
    dt_ready     = (dt_handling  + timedelta(hours=random.randint(4, 48))) if dt_handling else None
    dt_shipped   = (dt_ready     + timedelta(hours=random.randint(2, 24))) if dt_ready else None
    dt_delivered = (dt_shipped   + timedelta(days=random.randint(2, 12)))  if dt_shipped else None

    # ── _shipment_extra: já no formato de enriquecer_com_envio()
    # Isso evita chamadas reais à API durante testes.
    shipment_extra = {
        "shipping_status":          ship_status,
        "shipping_substatus":       "ready_to_ship" if ship_status in ("shipped", "delivered") else ship_status,
        "shipping_mode":            ship_mode,
        "shipping_service_id":      str(random.randint(100, 999)),
        "shipping_tracking_number": _rand_str(12),
        "shipping_tracking_method": random.choice(TRACK_METHODS),
        "shipping_date_handling":   _isodt(dt_handling),
        "shipping_date_ready":      _isodt(dt_ready),
        "shipping_date_shipped":    _isodt(dt_shipped),
        "shipping_date_delivered":  _isodt(dt_delivered),
        "receiver_address_line":    f"Rua {random.choice(['das Flores','do Comércio','Brasil','XV de Novembro'])}, {random.randint(1, 999)}",
        "receiver_zip":             _rand_cep(),
        "receiver_city":            cidade[0],
        "receiver_state":           cidade[0],
        "receiver_country":         "BR",
        "lead_time_cost":           ship_amount,
        "lead_time_name":           random.choice(["Normal", "Expresso", "Fulfillment", "Econômico"]),
    }

    pedido = {
        "id":               order_id,
        "status":           status,
        "status_detail":    "accredited"      if status == "paid"      else
                            "by_buyer"        if status == "cancelled" else
                            "pending_review",
        "date_created":     _isodt(dt_criacao),
        "date_approved":    _isodt(dt_aprovacao),
        "date_last_updated":_isodt(dt_last_upd),
        "date_closed":      _isodt(dt_fechamento),
        "expiration_date":  _isodt(dt_expira),
        "order_request":    {"return_requested": False, "change": None},
        "fulfilled":        random.choice([True, False, False]),
        "manufacturing_ending_date": None,
        "pack_id":          None,
        "coupon":           {"id": None, "amount": coupon_val},
        "currency_id":      "BRL",
        "total_amount":     total_amount,
        "paid_amount":      paid_amount,
        "amount_paid_to_seller": round(paid_amount - mkt_fee, 2),
        "shipping_amount":  ship_amount,
        "marketplace_fee":  mkt_fee,
        "taxes":            {"amount": taxes_val, "currency_id": "BRL", "rules": []},
        "tags":             random.sample(
                                ["paid", "not_delivered", "pack_order", "mshops", "test_order"],
                                k=random.randint(0, 2)
                            ),
        "context": {
            "channel": random.choice(CANAIS),
            "site":    "MLB",
            "flows":   [],
        },
        "buyer": {
            "id":           b["id"],
            "nickname":     b["nick"],
            "email":        b["email"],
            "phone":        {"area_code": b["tel"][:2], "number": b["tel"][2:]},
            "first_name":   b["fn"],
            "last_name":    b["ln"],
            "billing_info": {"doc_type": doc_tipo, "doc_number": doc_num},
        },
        "seller": {"id": SELLER_ID, "nickname": SELLER_NICK},
        "shipping": {
            "id":            ship_id,
            "shipping_mode": ship_mode,
            "logistic_type": logistic,
            "status":        ship_status,
            "date_created":  _isodt(dt_criacao),
        },
        "order_items": order_items,
        "payments": [{
            "id":                         order_id + 5_000_000_000,
            "order_id":                   order_id,
            "payer_id":                   b["id"],
            "collector":                  {"id": SELLER_ID},
            "card_id":                    None,
            "site_id":                    "MLB",
            "reason":                     " + ".join(p["title"][:30] for p in produtos_selecionados),
            "payment_method_id":          met_pag,
            "currency_id":                "BRL",
            "installments":               parcelas,
            "installment_rate":           round(random.uniform(0, 3.5), 2) if parcelas > 1 else 0,
            "transaction_amount":         total_amount,
            "transaction_amount_refunded":0,
            "coupon_amount":              coupon_val,
            "operation_type":             random.choice(OP_TYPES),
            "payment_type":               ptype,
            "status":                     "approved" if status == "paid"      else
                                          "cancelled" if status == "cancelled" else "pending",
            "status_detail":              "accredited" if status == "paid" else "pending_contingency",
            "date_approved":              _isodt(dt_aprovacao),
            "date_created":               _isodt(dt_criacao),
            "date_last_modified":         _isodt(dt_last_upd),
            "activation_uri":             None,
            "overpaid_amount":            0,
            "total_paid_amount":          paid_amount,
            "shipping_cost":              ship_amount,
            "taxes_amount":               taxes_val,
            "marketplace_fee":            mkt_fee,
            "reference_id":               None,
        }],
        # Chave especial: dados de envio pré-carregados (evita chamada real à API)
        "_shipment_extra": shipment_extra,
    }

    return pedido


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("\n" + "═" * 60)
    print("  Mercado Livre — Seed de Dados de Teste")
    print(f"  Pedidos a gerar: {N_PEDIDOS}")
    print(f"  Período: {_DATA_INI:%d/%m/%Y} → {_DATA_FIM:%d/%m/%Y}")
    print("═" * 60)

    engine = get_engine()
    criar_tabela_se_necessario(engine)

    if TRUNCATE:
        print("\n  [!] TRUNCATE=1 — limpando tabela bd_vendas_ml...")
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE bd_vendas_ml"))
            conn.commit()
        print("  ✔  Tabela limpa\n")

    # ── Gera IDs de pedido únicos (ordem temporal)
    base_order_id = 8_000_000_000
    segundos_range = int((_DATA_FIM - _DATA_INI).total_seconds())
    offsets = sorted(random.sample(range(segundos_range), N_PEDIDOS))

    pedidos = []
    for i, offset in enumerate(offsets):
        oid       = base_order_id + i + 1
        base_date = _DATA_INI + timedelta(seconds=offset)
        pedidos.append(gerar_pedido_fake(oid, base_date))

    print(f"\n  Gerados {len(pedidos)} pedidos fictícios")

    # ── Expande para linhas (sem chamar API — usa _shipment_extra)
    todas_linhas = []
    for p in pedidos:
        todas_linhas.extend(expandir_pedido(p, enrich_ship=False))

    df = pd.DataFrame(todas_linhas)
    df = df.drop_duplicates(subset=["order_id", "produto_id"])
    print(f"  Linhas geradas (itens): {len(df):,}")

    # ── Estatísticas rápidas
    status_counts = df.drop_duplicates("order_id")["status"].value_counts().to_dict()
    print(f"  Status: { {k: v for k, v in status_counts.items()} }")
    print(f"  Receita bruta total: R$ {df['receita_bruta'].sum():,.2f}")
    print(f"  Ticket médio: R$ {df.drop_duplicates('order_id')['total_venda_pedido'].mean():,.2f}")

    upsert_banco(engine, df)

    print("\n" + "═" * 60)
    print(f"  ✔  Seed concluído — {datetime.now():%d/%m/%Y %H:%M:%S}")
    print("  Rode: python -m mercadolivre.seed_test")
    print("  Para limpar antes: TRUNCATE=1 python -m mercadolivre.seed_test")
    print("═" * 60)


if __name__ == "__main__":
    from datetime import datetime
    main()
