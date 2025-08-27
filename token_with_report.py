from playwright.sync_api import sync_playwright
import time
import re

USERNAME = "ella.powell@xapien.com"
PASSWORD = "Xap_Generic2000*"
TOKEN_FILE = "token.txt"
REFRESH_INTERVAL = 15 * 60  # 15 minutes

def save_token(token):
    with open(TOKEN_FILE, "w") as f:
        f.write(token)
    print(f"💾 Token saved: {token[:30]}...")

def extract_token_from_headers(headers):
    for name, value in headers.items():
        if name.lower() == "authorization" and value.startswith("Bearer "):
            return value.split("Bearer ")[1]
    return None

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        def handle_request(route, request):
            token = extract_token_from_headers(request.headers)
            if token:
                save_token(token)
            route.continue_()

        context.route("**/*", handle_request)

        print("🌐 Opening Xapien portal...")
        page.goto("https://portal.xapien.com/")

        # Login process
        page.wait_for_selector('input[type="email"][placeholder="Email"]')
        page.fill('input[type="email"][placeholder="Email"]', USERNAME)
        page.click("button:has-text('Go')")

        page.wait_for_selector('input[type="password"][placeholder="Password"]')
        page.fill('input[type="password"][placeholder="Password"]', PASSWORD)
        page.click("button:has-text('Go')")

        # Wait for dashboard/search page
        page.wait_for_url(re.compile(r".*/search"), timeout=60000)
        print("✅ Logged in successfully.")

        # Refresh loop
        while True:
            print("🔄 Refreshing page...")
            page.reload()
            time.sleep(5)  # Give time for network calls

            # Force token refresh
            page.evaluate("fetch('/users').catch(()=>{});")

            print(f"⏳ Sleeping for {REFRESH_INTERVAL/60} minutes...")
            time.sleep(REFRESH_INTERVAL)

if __name__ == "__main__":
    main()
