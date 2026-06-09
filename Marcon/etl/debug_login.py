from playwright.sync_api import sync_playwright

PC_URL = "https://sys.precocerto.co"
PC_EMAIL = "comercial@casaeletromarcon.com.br"
PC_SENHA = "eletro123"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
        locale="pt-BR",
        viewport={"width": 1280, "height": 720}
    )
    page = context.new_page()
    page.goto(f"{PC_URL}/login/", wait_until="domcontentloaded")
    page.wait_for_timeout(2000)
    page.fill("input[name='username_login']", PC_EMAIL)
    page.fill("input[name='password_login']", PC_SENHA)
    page.click("button[type='submit']")
    page.wait_for_timeout(5000)
    print("URL atual:", page.url)
    page.screenshot(path="pos_login_test.png")
    print("Screenshot salvo em pos_login_test.png")
    browser.close()
