import pandas as pd
import json

with open('cache/orderlines_2026-05-01_2026-05-31.json', encoding='utf-8') as f:
    linhas = json.load(f)
with open('cache/orders_2026-05-01_2026-05-31.json', encoding='utf-8') as f:
    orders = json.load(f)

df = pd.DataFrame(linhas)
orders_df = pd.DataFrame(list(orders.values()))
df['order_id'] = pd.to_numeric(df['order'].apply(lambda x: x['id'] if isinstance(x, dict) else x), errors='coerce')
orders_df['order_id'] = pd.to_numeric(orders_df['id'], errors='coerce')

res = df.merge(orders_df[['order_id', 'status', 'total', 'source_created']], on='order_id', how='left')

print('--- Vendas por Status ---')
print(res.groupby('status')['total_sales'].sum())

