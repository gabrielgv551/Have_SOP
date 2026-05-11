"""
Mercado Livre — ETL de Vendas (schema completo)
================================================
Busca pedidos do ML e faz upsert na tabela bd_vendas_ml do PostgreSQL.
Enriquece cada pedido com detalhes de envio via GET /shipments/{id}.

USO:
  python -m mercadolivre.etl                   # incremental (desde último sync)
  FULL_RELOAD=1 python -m mercadolivre.etl     # recarga total desde FIRST_DATE
  ENRICH_SHIP=0 python -m mercadolivre.etl     # sem chamada /shipments (mais rápido)
"""

import io
import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import text
from mercadolivre.auth import get_access_token, carregar_tokens
from db import get_engine

load_dotenv()

ML_API       = "https://api.mercadolibre.com"
FIRST_DATE   = "2025-01-01"
FULL_RELOAD  = bool(os.getenv("FULL_RELOAD"))
OVERLAP_DAYS = 2
ENRICH_SHIP      = os.getenv("ENRICH_SHIP", "1") == "1"
ML_ACCOUNT_LABEL = os.getenv("ML_ACCOUNT_LABEL", "")  # ex: "Lanzi", "Marcon" — preenchido pelo worker


# ─────────────────────────────────────────────────────────────
# API MERCADO LIVRE
# ─────────────────────────────────────────────────────────────
def ml_get(path: str, params: dict = None, _retries: int = 5) -> dict:
    token = get_access_token()
    for attempt in range(_retries):
        resp = requests.get(
            f"{ML_API}{path}",
            headers={"Authorization": f"Bearer {token}"},
            params=params or {},
            timeout=20,
        )
        if resp.status_code == 429:
            wait = 60 * (attempt + 1)  # 60s, 120s, 180s, 240s, 300s
            print(f"\n  [429] Rate limit — aguardando {wait}s (tentativa {attempt+1}/{_retries})...")
            time.sleep(wait)
            continue
        if resp.status_code >= 500:
            wait = 10 * (attempt + 1)
            print(f"\n  [{resp.status_code}] Erro servidor — aguardando {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"ml_get: max retries atingido para {path}")


def get_user_id() -> str:
    tokens = carregar_tokens()
    if not tokens:
        raise RuntimeError(
            "Tokens não encontrados. Rode primeiro:\n"
            "  python -m mercadolivre.auth"
        )
    uid = tokens.get("user_id")
    if not uid:
        raise RuntimeError("user_id não encontrado nos tokens. Re-autorize com python -m mercadolivre.auth")
    return str(uid)


# ─────────────────────────────────────────────────────────────
# ENRIQUECIMENTO — ENVIO (/shipments/{id})
# ─────────────────────────────────────────────────────────────
def enriquecer_com_envio(shipping_id) -> dict:
    """Chama GET /shipments/{id} e devolve campos extras de envio."""
    if not shipping_id:
        return {}
    try:
        data    = ml_get(f"/shipments/{shipping_id}")
        rec     = data.get("receiver_address") or {}
        city    = rec.get("city") or {}
        state   = rec.get("state") or {}
        country = rec.get("country") or {}
        lead    = data.get("lead_time") or {}
        costs   = data.get("cost_components") or {}

        def _dt(val):
            return (val or "")[:19] or None

        return {
            "shipping_status":           data.get("status"),
            "shipping_substatus":        data.get("substatus"),
            "shipping_mode":             data.get("mode"),
            "shipping_service_id":       str(data.get("service_id") or ""),
            "shipping_tracking_number":  data.get("tracking_number"),
            "shipping_tracking_method":  data.get("tracking_method"),
            "shipping_date_handling":    _dt(data.get("date_handling")),
            "shipping_date_ready":       _dt(data.get("date_ready_to_ship")),
            "shipping_date_shipped":     _dt(data.get("date_shipped")),
            "shipping_date_delivered":   _dt(data.get("date_delivered")),
            "receiver_address_line":     rec.get("address_line"),
            "receiver_zip":              rec.get("zip_code"),
            "receiver_city":             city.get("name") if isinstance(city, dict) else str(city or ""),
            "receiver_state":            state.get("name") if isinstance(state, dict) else str(state or ""),
            "receiver_country":          country.get("id") if isinstance(country, dict) else str(country or ""),
            "lead_time_cost":            float(lead.get("cost") or 0),
            "lead_time_name":            lead.get("name"),
            "frete_pago":               float(costs.get("special_value") or costs.get("gross_value") or 0),
            "frete_bruto":              float(costs.get("gross_value") or 0),
        }
    except Exception as e:
        print(f"\n  [WARN] /shipments/{shipping_id}: {e}")
        return {}


# ─────────────────────────────────────────────────────────────
# BUSCAR PEDIDOS
# ─────────────────────────────────────────────────────────────
def _buscar_chunk(user_id: str, data_ini: str, data_fim: str, total_global: list) -> list:
    """Busca pedidos de um intervalo de datas (máx 10.000 resultados por chunk)."""
    pedidos = []
    offset  = 0
    limit   = 50
    MAX_OFFSET = 9950  # limite seguro antes do 400 do ML

    while True:
        data = ml_get("/orders/search", params={
            "seller":                  user_id,
            "order.date_created.from": f"{data_ini}T00:00:00.000-03:00",
            "order.date_created.to":   f"{data_fim}T23:59:59.000-03:00",
            "sort":                    "date_asc",
            "offset":                  offset,
            "limit":                   limit,
        })

        resultados = data.get("results", [])
        if not resultados:
            break

        pedidos.extend(resultados)
        total_chunk = data.get("paging", {}).get("total", 0)
        offset += limit

        sys.stdout.write(f"\r  Buscando pedidos: {total_global[0] + len(pedidos)}/{total_global[1]}   ")
        sys.stdout.flush()

        if offset >= total_chunk or offset > MAX_OFFSET:
            break
        time.sleep(0.3)

    return pedidos


def buscar_pedidos(user_id: str, data_ini: str, data_fim: str) -> list:
    """
    Busca todos os pedidos do período, iterando mês a mês para
    respeitar o limite de offset=10.000 da API do Mercado Livre.
    """
    from datetime import date, timedelta
    import calendar

    ini = datetime.strptime(data_ini, "%Y-%m-%d").date()
    fim = datetime.strptime(data_fim, "%Y-%m-%d").date()

    # Gerar chunks mensais
    chunks = []
    cur = ini.replace(day=1)
    while cur <= fim:
        ultimo_dia = calendar.monthrange(cur.year, cur.month)[1]
        chunk_ini = max(cur, ini)
        chunk_fim = min(date(cur.year, cur.month, ultimo_dia), fim)
        chunks.append((chunk_ini.strftime("%Y-%m-%d"), chunk_fim.strftime("%Y-%m-%d")))
        # Avançar para o próximo mês
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)

    # Descobrir total estimado (primeiro chunk, só para o print)
    try:
        primeiro = ml_get("/orders/search", params={
            "seller":                  user_id,
            "order.date_created.from": f"{data_ini}T00:00:00.000-03:00",
            "order.date_created.to":   f"{data_fim}T23:59:59.000-03:00",
            "sort":                    "date_asc",
            "offset":                  0,
            "limit":                   1,
        })
        total_estimado = primeiro.get("paging", {}).get("total", 0)
    except Exception:
        total_estimado = 0

    print(f"\n  Total estimado: {total_estimado:,} pedidos em {len(chunks)} chunk(s) mensal(is)")

    total_global = [0, total_estimado]  # [buscados_até_agora, total]
    pedidos = []

    for chunk_ini, chunk_fim in chunks:
        chunk = _buscar_chunk(user_id, chunk_ini, chunk_fim, total_global)
        pedidos.extend(chunk)
        total_global[0] = len(pedidos)
        time.sleep(0.2)

    print()
    return pedidos


def buscar_pedidos_incrementais(user_id: str, desde: str) -> list:
    """
    Incremental: busca pedidos CRIADOS OU ATUALIZADOS desde `desde` (YYYY-MM-DD).
    Usa order.last_updated — captura novos pedidos e mudanças de status.
    Não precisa de chunking mensal pois o intervalo é curto (horas/dias).
    """
    hoje = datetime.now().strftime("%Y-%m-%d")

    # Total estimado
    try:
        r = ml_get("/orders/search", params={
            "seller":                   user_id,
            "order.last_updated.from":  f"{desde}T00:00:00.000-03:00",
            "order.last_updated.to":    f"{hoje}T23:59:59.000-03:00",
            "sort":                     "last_updated_asc",
            "offset":                   0,
            "limit":                    1,
        })
        total = r.get("paging", {}).get("total", 0)
    except Exception:
        total = 0

    print(f"\n  [Incremental] Pedidos atualizados desde {desde}: {total:,}")

    pedidos = []
    offset  = 0
    limit   = 50
    total_global = [0, total]

    while True:
        data = ml_get("/orders/search", params={
            "seller":                   user_id,
            "order.last_updated.from":  f"{desde}T00:00:00.000-03:00",
            "order.last_updated.to":    f"{hoje}T23:59:59.000-03:00",
            "sort":                     "last_updated_asc",
            "offset":                   offset,
            "limit":                    limit,
        })
        resultados = data.get("results", [])
        if not resultados:
            break
        pedidos.extend(resultados)
        offset += limit
        total_global[0] = len(pedidos)
        sys.stdout.write(f"\r  Buscando pedidos: {len(pedidos)}/{total}   ")
        sys.stdout.flush()
        if offset >= total or offset > 9950:
            break
        time.sleep(0.3)

    print()
    return pedidos


# ─────────────────────────────────────────────────────────────
# EXPANDIR PEDIDO → LINHAS (schema completo)
# ─────────────────────────────────────────────────────────────
def expandir_pedido(pedido: dict, enrich_ship: bool = True) -> list:
    """
    Transforma um objeto de pedido da API do ML em uma lista de linhas
    (uma por item), com todos os campos disponíveis.

    Se o pedido contiver a chave "_shipment_extra" (dict já no formato
    de enriquecer_com_envio), ela é usada diretamente sem chamar a API.
    Isso permite injetar dados de envio em testes sem chamadas reais.
    """
    linhas = []

    def _f(val, default=0.0):
        if val is None:
            return default
        if isinstance(val, dict):
            val = val.get("value", default)
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    def _s(val):
        return str(val) if val is not None else None

    def _d(val, n=10):
        return (val or "")[:n] or None

    # ── Identificação
    order_id = pedido.get("id")

    # ── Datas do pedido
    data_criacao      = _d(pedido.get("date_created"))
    data_aprovacao    = _d(pedido.get("date_approved"))
    data_fechamento   = _d(pedido.get("date_closed"))
    data_last_updated = _d(pedido.get("date_last_updated"), 19)
    data_expiracao    = _d(pedido.get("expiration_date"), 19)

    # ── Status
    status        = pedido.get("status", "")
    status_det    = pedido.get("status_detail", "")
    tags          = ",".join(pedido.get("tags") or [])
    fulfilled     = bool(pedido.get("fulfilled"))
    pack_id       = _s(pedido.get("pack_id"))

    # ── Contexto (canal de venda)
    ctx           = pedido.get("context") or {}
    context_canal = ctx.get("channel", "")
    context_site  = ctx.get("site", "")

    # ── Order request
    req_devolucao = bool((pedido.get("order_request") or {}).get("return_requested"))

    # ── Moeda
    moeda = pedido.get("currency_id", "BRL")

    # ── Valores financeiros do pedido
    total_venda_pedido    = _f(pedido.get("total_amount"))
    paid_amount           = _f(pedido.get("paid_amount"))
    amount_paid_to_seller = _f(pedido.get("amount_paid_to_seller"))
    frete_recebido        = _f(pedido.get("shipping_amount"))
    taxa_ml               = _f(pedido.get("marketplace_fee"))
    taxes_amount          = _f((pedido.get("taxes") or {}).get("amount"))
    coupon_amount         = _f((pedido.get("coupon") or {}).get("amount"))
    repasse_financeiro    = round(total_venda_pedido - taxa_ml, 2)

    # ── Comprador
    buyer         = pedido.get("buyer") or {}
    billing       = buyer.get("billing_info") or pedido.get("billing_info") or {}
    buyer_id      = buyer.get("id")
    cliente       = buyer.get("nickname", "")
    buyer_email   = buyer.get("email", "")
    buyer_fname   = buyer.get("first_name", "")
    buyer_lname   = buyer.get("last_name", "")
    buyer_phone   = str((buyer.get("phone") or {}).get("number") or "")
    doc_tipo      = billing.get("doc_type", "")
    doc_numero    = billing.get("doc_number", "")

    # ── Vendedor
    seller      = pedido.get("seller") or {}
    seller_id   = seller.get("id")
    seller_nick = seller.get("nickname", "")

    # ── Envio (campos básicos do objeto de pedido)
    shipping      = pedido.get("shipping") or {}
    shipping_id   = shipping.get("id")
    metodo_envio  = shipping.get("shipping_mode", "")
    logistic_type = shipping.get("logistic_type", "")
    shipping_dc   = _d(shipping.get("date_created"), 19)

    # ── Envio enriquecido — usa _shipment_extra (teste) ou chama API (produção)
    ship_extra = pedido.get("_shipment_extra") or {}
    if not ship_extra and enrich_ship and shipping_id:
        ship_extra = enriquecer_com_envio(shipping_id)

    # ── Pagamento (primeiro da lista)
    payments              = pedido.get("payments") or []
    payment               = payments[0] if payments else {}
    payment_id            = _s(payment.get("id"))
    payer_id              = _s(payment.get("payer_id"))
    metodo_pagamento      = payment.get("payment_method_id", "")
    payment_type          = payment.get("payment_type", "")
    payment_op_type       = payment.get("operation_type", "")
    parcelas              = int(payment.get("installments") or 1)
    installment_rate      = _f(payment.get("installment_rate"))
    transaction_amount    = _f(payment.get("transaction_amount"))
    total_paid_amount     = _f(payment.get("total_paid_amount"))
    trans_amount_refunded = _f(payment.get("transaction_amount_refunded"))
    payment_coupon        = _f(payment.get("coupon_amount"))
    payment_ship_cost     = _f(payment.get("shipping_cost"))
    payment_taxes         = _f(payment.get("taxes_amount"))
    payment_mkt_fee       = _f(payment.get("marketplace_fee"))
    overpaid_amount       = _f(payment.get("overpaid_amount"))
    status_pagamento      = payment.get("status", "")
    payment_status_det    = payment.get("status_detail", "")
    payment_date_created  = _d(payment.get("date_created"), 19)
    payment_date_approved = _d(payment.get("date_approved"), 19)
    payment_date_modified = _d(payment.get("date_last_modified"), 19)

    # ── Itens do pedido
    for item in pedido.get("order_items") or []:
        it = item.get("item") or {}

        produto_id     = it.get("id", "")
        nome_produto   = it.get("title", "")
        sku            = it.get("seller_sku") or it.get("seller_custom_field") or ""
        categoria_id   = it.get("category_id", "")
        condicao       = it.get("condition", "")
        listing_type   = it.get("listing_type_id", "")
        variacao_id    = _s(it.get("variation_id"))
        variacao_attrs = str(it.get("variation_attributes") or "")
        item_peso      = _f(it.get("net_weight"))
        item_garantia  = str(it.get("warranty") or "")

        req_qty        = item.get("requested_quantity") or {}
        quantidade     = _f(item.get("quantity"))
        qtd_solicitada = _f(req_qty.get("value") or item.get("quantity"))
        qtd_coletada   = _f(item.get("picked_quantity"))
        preco_unit     = _f(item.get("unit_price"))
        preco_orig     = _f(item.get("full_unit_price") or preco_unit)
        desconto_unit  = round(preco_orig - preco_unit, 4)
        total_item     = round(quantidade * preco_unit, 2)
        sale_fee       = _f(item.get("sale_fee"))
        manuf_days     = int(item.get("manufacturing_days") or 0)

        # ── DRE rateado pelo peso do item no pedido
        ratio         = (total_item / total_venda_pedido) if total_venda_pedido else 0
        receita_bruta = total_item
        comissao_item = round(taxa_ml * ratio, 4)
        frete_item    = round(frete_recebido * ratio, 4)
        valor_liquido = round(total_item - comissao_item, 2)

        ano = int(data_criacao[:4]) if data_criacao else None
        mes = int(data_criacao[5:7]) if data_criacao else None

        linhas.append({
            # ── Identificação
            "order_id":                  order_id,
            "numero_ecommerce":          _s(order_id),
            "produto_id":                produto_id,
            "pack_id":                   pack_id,
            # ── Datas do pedido
            "data":                      data_criacao,
            "data_aprovacao":            data_aprovacao,
            "data_fechamento":           data_fechamento,
            "data_last_updated":         data_last_updated,
            "data_expiracao":            data_expiracao,
            "ano":                       ano,
            "mes":                       mes,
            # ── Status
            "status":                    status,
            "status_detalhe":            status_det,
            "tags":                      tags,
            "fulfilled":                 fulfilled,
            "context_canal":             context_canal,
            "context_site":              context_site,
            "order_request_devolucao":   req_devolucao,
            # ── Comprador
            "cliente":                   cliente,
            "buyer_id":                  buyer_id,
            "buyer_email":               buyer_email,
            "buyer_primeiro_nome":       buyer_fname,
            "buyer_ultimo_nome":         buyer_lname,
            "buyer_telefone":            buyer_phone,
            "doc_tipo":                  doc_tipo,
            "doc_numero":                doc_numero,
            # ── Vendedor
            "seller_id":                 seller_id,
            "seller_nickname":           seller_nick,
            # ── Produto / Item
            "sku":                       sku,
            "nome_produto":              nome_produto,
            "categoria_id":              categoria_id,
            "condicao":                  condicao,
            "listing_type_id":           listing_type,
            "variacao_id":               variacao_id,
            "variacao_attrs":            variacao_attrs,
            "item_peso":                 item_peso,
            "item_garantia":             item_garantia,
            # ── Quantidades
            "quantidade":                quantidade,
            "qtd_solicitada":            qtd_solicitada,
            "qtd_coletada":              qtd_coletada,
            # ── Preços
            "moeda":                     moeda,
            "preco_unitario":            preco_unit,
            "preco_original":            preco_orig,
            "desconto_unitario":         desconto_unit,
            "sale_fee":                  sale_fee,
            "manufacturing_days":        manuf_days,
            # ── Valores do pedido
            "total_item":                total_item,
            "total_venda_pedido":        total_venda_pedido,
            "paid_amount":               paid_amount,
            "amount_paid_to_seller":     amount_paid_to_seller,
            "frete_recebido":            frete_recebido,
            "taxa_ml":                   taxa_ml,
            "coupon_amount":             coupon_amount,
            "taxes_amount":              taxes_amount,
            "repasse_financeiro":        repasse_financeiro,
            # ── DRE (rateado por item)
            "receita_bruta":             receita_bruta,
            "comissao_item":             comissao_item,
            "frete_item":                frete_item,
            "valor_liquido":             valor_liquido,
            # ── Pagamento
            "payment_id":                payment_id,
            "payer_id":                  payer_id,
            "metodo_pagamento":          metodo_pagamento,
            "payment_type":              payment_type,
            "payment_op_type":           payment_op_type,
            "parcelas":                  parcelas,
            "installment_rate":          installment_rate,
            "transaction_amount":        transaction_amount,
            "total_paid_amount":         total_paid_amount,
            "trans_amount_refunded":     trans_amount_refunded,
            "payment_coupon_amount":     payment_coupon,
            "payment_ship_cost":         payment_ship_cost,
            "payment_taxes_amount":      payment_taxes,
            "payment_marketplace_fee":   payment_mkt_fee,
            "overpaid_amount":           overpaid_amount,
            "status_pagamento":          status_pagamento,
            "payment_status_detail":     payment_status_det,
            "payment_date_created":      payment_date_created,
            "payment_date_approved":     payment_date_approved,
            "payment_date_modified":     payment_date_modified,
            # ── Envio (básico — objeto do pedido)
            "shipping_id":               _s(shipping_id),
            "metodo_envio":              metodo_envio,
            "logistic_type":             logistic_type,
            "shipping_date_criacao":     shipping_dc,
            # ── Envio (enriquecido — /shipments/{id})
            "shipping_status":           ship_extra.get("shipping_status"),
            "shipping_substatus":        ship_extra.get("shipping_substatus"),
            "shipping_mode":             ship_extra.get("shipping_mode"),
            "shipping_service_id":       ship_extra.get("shipping_service_id"),
            "shipping_tracking_number":  ship_extra.get("shipping_tracking_number"),
            "shipping_tracking_method":  ship_extra.get("shipping_tracking_method"),
            "shipping_date_handling":    ship_extra.get("shipping_date_handling"),
            "shipping_date_ready":       ship_extra.get("shipping_date_ready"),
            "shipping_date_shipped":     ship_extra.get("shipping_date_shipped"),
            "shipping_date_delivered":   ship_extra.get("shipping_date_delivered"),
            "receiver_address_line":     ship_extra.get("receiver_address_line"),
            "receiver_zip":              ship_extra.get("receiver_zip"),
            "receiver_city":             ship_extra.get("receiver_city"),
            "receiver_state":            ship_extra.get("receiver_state"),
            "receiver_country":          ship_extra.get("receiver_country"),
            "lead_time_cost":            ship_extra.get("lead_time_cost", 0.0),
            "lead_time_name":            ship_extra.get("lead_time_name"),
            "frete_pago":               ship_extra.get("frete_pago", 0.0),
            "frete_bruto":              ship_extra.get("frete_bruto", 0.0),
            # ── Meta
            "canal":                     (f"ML Full {ML_ACCOUNT_LABEL}".strip() if fulfilled else "Mercado Livre"),
        })

    return linhas


# ─────────────────────────────────────────────────────────────
# CRIAR TABELA SE NÃO EXISTIR
# ─────────────────────────────────────────────────────────────
def criar_tabela_se_necessario(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bd_vendas_ml (
                -- Identificação
                order_id                  BIGINT,
                numero_ecommerce          TEXT,
                produto_id                TEXT,
                pack_id                   TEXT,
                -- Datas do pedido
                data                      DATE,
                data_aprovacao            DATE,
                data_fechamento           DATE,
                data_last_updated         TIMESTAMP,
                data_expiracao            TIMESTAMP,
                ano                       INTEGER,
                mes                       INTEGER,
                -- Status
                status                    TEXT,
                status_detalhe            TEXT,
                tags                      TEXT,
                fulfilled                 BOOLEAN,
                context_canal             TEXT,
                context_site              TEXT,
                order_request_devolucao   BOOLEAN,
                -- Comprador
                cliente                   TEXT,
                buyer_id                  BIGINT,
                buyer_email               TEXT,
                buyer_primeiro_nome       TEXT,
                buyer_ultimo_nome         TEXT,
                buyer_telefone            TEXT,
                doc_tipo                  TEXT,
                doc_numero                TEXT,
                -- Vendedor
                seller_id                 BIGINT,
                seller_nickname           TEXT,
                -- Produto / Item
                sku                       TEXT,
                nome_produto              TEXT,
                categoria_id              TEXT,
                condicao                  TEXT,
                listing_type_id           TEXT,
                variacao_id               TEXT,
                variacao_attrs            TEXT,
                item_peso                 NUMERIC,
                item_garantia             TEXT,
                -- Quantidades
                quantidade                NUMERIC,
                qtd_solicitada            NUMERIC,
                qtd_coletada              NUMERIC,
                -- Preços
                moeda                     TEXT,
                preco_unitario            NUMERIC,
                preco_original            NUMERIC,
                desconto_unitario         NUMERIC,
                sale_fee                  NUMERIC,
                manufacturing_days        INTEGER,
                -- Valores do pedido
                total_item                NUMERIC,
                total_venda_pedido        NUMERIC,
                paid_amount               NUMERIC,
                amount_paid_to_seller     NUMERIC,
                frete_recebido            NUMERIC,
                taxa_ml                   NUMERIC,
                coupon_amount             NUMERIC,
                taxes_amount              NUMERIC,
                repasse_financeiro        NUMERIC,
                -- DRE (rateado por item)
                receita_bruta             NUMERIC,
                comissao_item             NUMERIC,
                frete_item                NUMERIC,
                valor_liquido             NUMERIC,
                -- Pagamento
                payment_id                TEXT,
                payer_id                  TEXT,
                metodo_pagamento          TEXT,
                payment_type              TEXT,
                payment_op_type           TEXT,
                parcelas                  INTEGER,
                installment_rate          NUMERIC,
                transaction_amount        NUMERIC,
                total_paid_amount         NUMERIC,
                trans_amount_refunded     NUMERIC,
                payment_coupon_amount     NUMERIC,
                payment_ship_cost         NUMERIC,
                payment_taxes_amount      NUMERIC,
                payment_marketplace_fee   NUMERIC,
                overpaid_amount           NUMERIC,
                status_pagamento          TEXT,
                payment_status_detail     TEXT,
                payment_date_created      TIMESTAMP,
                payment_date_approved     TIMESTAMP,
                payment_date_modified     TIMESTAMP,
                -- Envio (básico)
                shipping_id               TEXT,
                metodo_envio              TEXT,
                logistic_type             TEXT,
                shipping_date_criacao     TIMESTAMP,
                -- Envio (enriquecido via /shipments/{id})
                shipping_status           TEXT,
                shipping_substatus        TEXT,
                shipping_mode             TEXT,
                shipping_service_id       TEXT,
                shipping_tracking_number  TEXT,
                shipping_tracking_method  TEXT,
                shipping_date_handling    TIMESTAMP,
                shipping_date_ready       TIMESTAMP,
                shipping_date_shipped     TIMESTAMP,
                shipping_date_delivered   TIMESTAMP,
                receiver_address_line     TEXT,
                receiver_zip              TEXT,
                receiver_city             TEXT,
                receiver_state            TEXT,
                receiver_country          TEXT,
                lead_time_cost            NUMERIC,
                lead_time_name            TEXT,
                frete_pago                NUMERIC DEFAULT 0,
                frete_bruto               NUMERIC DEFAULT 0,
                -- Meta
                canal                     TEXT,
                PRIMARY KEY (order_id, produto_id)
            )
        """))
        conn.commit()


# ─────────────────────────────────────────────────────────────
# UPSERT NO BANCO
# ─────────────────────────────────────────────────────────────
def upsert_banco(engine, df: pd.DataFrame):
    print(f"  Salvando {len(df):,} registros no PostgreSQL (upsert)...")

    cols        = list(df.columns)
    cols_quoted = ", ".join(f'"{c}"' for c in cols)
    update_set  = ",\n                    ".join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in cols if c not in ("order_id", "produto_id")
    )

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE _tmp_ml AS SELECT * FROM bd_vendas_ml LIMIT 0"
            )

            buf = io.StringIO()
            df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N",
                      date_format="%Y-%m-%d")
            buf.seek(0)
            cur.copy_expert(
                f'COPY _tmp_ml ({cols_quoted}) FROM STDIN '
                f"WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                buf,
            )

            cur.execute(f"""
                INSERT INTO bd_vendas_ml ({cols_quoted})
                SELECT {cols_quoted} FROM _tmp_ml
                ON CONFLICT (order_id, produto_id) DO UPDATE SET
                    {update_set}
            """)

        raw.commit()
        print(f"  ✔  Upsert concluído — {len(df):,} registros")
    finally:
        raw.close()


# ─────────────────────────────────────────────────────────────
# DATA DO ÚLTIMO SYNC
# ─────────────────────────────────────────────────────────────
def obter_data_ultimo_sync(engine) -> str:
    try:
        with engine.connect() as conn:
            resultado = conn.execute(
                text("SELECT MAX(data) FROM bd_vendas_ml")
            ).scalar()
        if resultado:
            data_base = pd.to_datetime(resultado) - timedelta(days=OVERLAP_DAYS)
            data_str  = data_base.strftime("%Y-%m-%d")
            print(f"  Último registro no banco: {pd.to_datetime(resultado).strftime('%d/%m/%Y')}")
            print(f"  Buscando desde: {data_str} (sobreposição de {OVERLAP_DAYS}d)")
            return data_str
    except Exception:
        pass
    print("  Tabela vazia ou inexistente → carga inicial completa")
    return FIRST_DATE


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("\n" + "═" * 60)
    print("  Mercado Livre ETL")
    print(f"  Início: {datetime.now():%d/%m/%Y %H:%M:%S}")
    print(f"  Enriquecer envios: {'SIM' if ENRICH_SHIP else 'NÃO'}")
    print(f"  Conta (Matriz/Filial): {ML_ACCOUNT_LABEL}")
    print("═" * 60)

    engine  = get_engine()
    user_id = get_user_id()
    print(f"\n  User ID: {user_id}")

    criar_tabela_se_necessario(engine)

    end_date = datetime.today().strftime("%Y-%m-%d")

    if FULL_RELOAD:
        first_date = FIRST_DATE
        print(f"  Modo: FULL RELOAD desde {FIRST_DATE}")
    else:
        first_date = obter_data_ultimo_sync(engine)

    print(f"  Período: {first_date} → {end_date}")

    pedidos = buscar_pedidos(user_id, first_date, end_date)
    print(f"  Total pedidos encontrados: {len(pedidos)}")

    if not pedidos:
        print("  Nenhum pedido encontrado.")
        return

    todas_linhas = []
    for i, p in enumerate(pedidos, 1):
        sys.stdout.write(f"\r  Expandindo pedido {i}/{len(pedidos)}   ")
        sys.stdout.flush()
        todas_linhas.extend(expandir_pedido(p, enrich_ship=ENRICH_SHIP))
    print()

    df = pd.DataFrame(todas_linhas)
    df = df.drop_duplicates(subset=["order_id", "produto_id"])
    print(f"  Linhas geradas: {len(df):,}")

    upsert_banco(engine, df)

    print("═" * 60)
    print(f"  ✔  ETL finalizado — {datetime.now():%d/%m/%Y %H:%M:%S}")
    print("═" * 60)

    try:
        from consolidar import status_consolidado
        status_consolidado(engine)
    except Exception as _e:
        print(f"  [aviso] consolidado: {_e}")


if __name__ == "__main__":
    main()
