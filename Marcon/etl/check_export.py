import requests, pandas as pd

url = "https://sys-precocerto-co-django.s3.amazonaws.com/media/orders-by-product-sheet-exported/13555/pedidos_26-05-2026-15-43-30.xlsx?AWSAccessKeyId=ASIASRDYSH5ERFQXJZZE&Signature=packdc42P%2FjG2VLxhR7INYTAMvE%3D&x-amz-security-token=IQoJb3JpZ2luX2VjELj%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCICTCHW7Wk69VCS6WN%2FLLl7wRHiQ%2BFjMAzuoduC1Ipz%2FWAiEAl9XhGpyBl%2F9ydg9rfkxT6tE7y56vLh2T%2Bitb92BMQDcqzAQIgf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARADGgwxNzQxOTkwMjk1NzciDGR%2BYnhLvCSwvxfZYiqgBEzi90d%2Fd8iJK%2BQMGfAE3EP1BNnuSiHP1j3vccehWdsqW9AJDxYPJ7IXzYhR2Cl%2FoKk3AzcshmVHsNVrxNbhPmZvUvp7LGG9ajnEnx7e4cbESxFJUCTitlSQ9BRJ6vICx9a3MJge%2BZR2vZr3vV2r%2FivBf3%2FNBLQ25XBEf0%2FEmWvXSyQhwMuU4%2FOSnEknkfz0P2IKlRZ2cQ%2FX7oJC%2FMfVo0rG44%2FXWvgrXkCcRN0SoRl%2BGZwO7RZbY3OXFoKuz%2Bt23kzeDhFuoS8OI5mgd1C0DCsFQc0TT1iBK6tS99Nb9Czq9yk8m0RVvb7KnXRGwu6d0P3IoHnKIwDcHWpNWXyroA%2FOdaHm0vvLFF2NyMBCDCeSlrHBnavxQFKTk5SFXnBwl9NecAZxap2GgXuSk%2B6q358I7kBDenqUf%2BxJ68Eyy%2FvPTDP3UxeBdqN3tDy5gNc5OckRvJXNrMluLhBZLq8WX44QYe%2FX%2BEVq2brq5k9AZrxZcdOYbN%2FWFtEa48Y0zVV64rnMlBz3X5ZDtxQzccLFLrhwcDmOld35%2FbkNNl3%2Bh4%2B%2FoUAIA%2BKrcOhobbIB1NjFybsJbIdXzfSr%2Fqx2cpIfus2B5RMI9q3r1gfgN0w94X0FHl1HUpgN79jRg9a%2BPMxXUZmDeiFa%2B02C2S%2BU8DVODvT6HooUUoAw1pxKpvrf37on%2BG68JEg35SAyMj4ja8nlj1s0wQezQ4B6%2F4e5pTfbURYwqvfW0AY6pgE4a3U5KjHkPSrC8e2jiVmadDhgt5Ru1ucM4dnk5MzQX7zoQHWUYC4EzyDzrH1lRyzt%2FYU1C79uhGkxeCZJrG8VRiQj0%2FPlnN9kr7diAl8HCYy9c9dBLXyVhPimaRVeRr3xdF0QmKR%2FVUvkCxOq63GI2f1n%2FzmtqrwZpSONQkqKu7yOrnf%2B9fRkXEng6a9cchH92JeeN%2BGCg75TZmMJVlqPk15%2BSsXC&Expires=1779812092"

r = requests.get(url, timeout=30)
print(f"Status: {r.status_code} | Size: {len(r.content):,} bytes")

with open("export_sample.xlsx", "wb") as f:
    f.write(r.content)

df = pd.read_excel("export_sample.xlsx")
print(f"\nShape: {df.shape[0]:,} linhas x {df.shape[1]} colunas")
print("\nColunas:")
for col in df.columns:
    print(f"  {col}")
print("\nPrimeira linha:")
print(df.iloc[0].to_dict())
