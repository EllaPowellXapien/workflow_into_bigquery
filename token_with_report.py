from playwright.sync_api import sync_playwright
import re

USERNAME = "your_username_here"
PASSWORD = "your_password_here"
MAX_REFRESHES = 1  # Only refresh once by default

def extract_token_from_headers(headers):
    # Your existing extraction logic
    pass

def save_token(token):
    # Your existing token saving logic
    pass

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
    page.wait_for_selector('input[type="email"][placeholder="Email"]')
    page.fill('input[type="email"][placeholder="Email"]', USERNAME)
    page.click("button:has-text('Go')")
    page.wait_for_selector('input[type="password"][placeholder="Password"]')
    page.fill('input[type="password"][placeholder="Password"]', PASSWORD)
    page.click("button:has-text('Go')")
    page.wait_for_url(re.compile(r".*/search"), timeout=60000)
    print("✅ Logged in successfully.")
    for _ in range(MAX_REFRESHES):
        print("🔄 Refreshing page...")
        page.reload()
