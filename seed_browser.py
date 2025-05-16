#!/usr/bin/env python3
import os
import sys
import json
import pickle
import undetected_chromedriver as uc

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

def main():
    # 1) load config.json and your Zillow URL
    if not os.path.exists(CONFIG_PATH):
        print(f"❌  {CONFIG_PATH} not found. Create it and add your Zillow URL under 'zillow_url'.")
        sys.exit(1)
    with open(CONFIG_PATH) as f:
        cfg = json.load(f)

    url = cfg.get("zillow_url")
    if not url:
        print("❌  Add your Zillow URL to config.json under 'zillow_url' and run again.")
        sys.exit(1)

    # 2) launch a headful, “undetected” Chrome
    options = uc.ChromeOptions()
    options.headless = False
    # stability flags
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    # (optional) reuse your real Chrome profile to carry persistent cookies:
    # profile = os.path.expanduser("~/Library/Application Support/Google/Chrome")
    # options.add_argument(f"--user-data-dir={profile}")
    # options.add_argument("--profile-directory=Default")

    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(60)

    # 3) load Zillow and let you solve the CAPTCHA
    print(f"🔎  Opening Zillow: {url}")
    driver.get(url)

    print("""
🔔  **Please solve the CAPTCHA in the browser window**  
  • Click‐and‐hold the puzzle slider until it completes  
  • Wait for the listings page to fully load (white→Zillow UI)  
  • Then come back here and press ENTER
    """.strip())
    input()

    # 4) save the cookies out and exit
    cookies = driver.get_cookies()
    with open("cookies.pkl", "wb") as f:
        pickle.dump(cookies, f)
    print("✅  cookies.pkl saved. Exiting.")
    driver.quit()

if __name__ == "__main__":
    main()

