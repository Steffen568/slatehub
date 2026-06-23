#!/usr/bin/env python3
"""
download_fangraphs.py — Download FanGraphs CSVs via Playwright.

First run: opens a browser window, you log in to FanGraphs manually, then
press Enter. The session is saved to fg_auth.json for all future runs.

Subsequent runs: fully automatic, no interaction needed.

Setup (one-time):
  py -3.12 -m pip install playwright
  py -3.12 -m playwright install chromium

Usage:
  py -3.12 download_fangraphs.py
  py -3.12 load_fangraphs_excel.py
"""

import os, time, urllib.parse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from config import SEASON

DOWNLOAD_DIR = os.path.join(os.path.expanduser('~'), 'Desktop', 'WebDev', 'MLB_PQs')
AUTH_FILE    = os.path.join(os.path.dirname(__file__), 'fg_auth.json')
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

S = str(SEASON)

PAGES = [

    # ── Pitcher leaderboards ──────────────────────────────────────────────────
    ("Dash",
     f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=pit&lg=all&type=8&season={S}&month=0&season1={S}&ind=0&pageitems=2000000000&qual=1"),

    ("BattedBall",
     f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=pit&lg=all&type=2&season={S}&month=0&season1={S}&ind=0&pageitems=2000000000&qual=1"),

    ("Pitcehr Advanced",
     f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=pit&lg=all&type=1&season={S}&month=0&season1={S}&ind=0&qual=1&pagenum=1&pageitems=2000000000"),

    ("Plate Discipline",
     f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=pit&lg=all&type=22&season={S}&month=0&season1={S}&ind=0&qual=1&pageitems=2000000000"),

    ("Stuff+",
     f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=pit&lg=all&type=36&season={S}&month=0&season1={S}&ind=0&pageitems=2000000000&qual=1"),

    ("Location+",
     f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=pit&lg=all&type=37&season={S}&month=0&season1={S}&ind=0&pageitems=2000000000&qual=1"),

    ("Pitching+",
     f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=pit&lg=all&type=38&season={S}&month=0&season1={S}&ind=0&pageitems=2000000000&qual=1"),

    # ── Batter leaderboards ───────────────────────────────────────────────────
    ("HitterDash",
     f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=bat&lg=all&type=8&season={S}&month=0&season1={S}&ind=0&pageitems=2000000000&qual=1"),

    ("HitterStatcas",
     f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=bat&lg=all&type=24&season={S}&month=0&season1={S}&ind=0&pageitems=2000000000&qual=1"),

    ("BatTracking",
     f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=bat&lg=all&type=80&season={S}&month=0&season1={S}&ind=0&pageitems=2000000000&qual=1"),

    # ── Splits leaderboards ───────────────────────────────────────────────────
    ("vLHP",
     f"https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=1&splitArrPitch=&autoPt=false&splitTeams=false&statType=player&statgroup=2&startDate={S}-03-01&endDate={S}-11-01&players=&filter=PA%7Cgt%7C1&groupBy=season&wxTemperature=&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&position=B&sort=22,1&pageitems=2000000000&pg=0"),

    ("vRHP",
     f"https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=2&splitArrPitch=&autoPt=false&splitTeams=false&statType=player&statgroup=2&startDate={S}-03-01&endDate={S}-11-01&players=&filter=PA%7Cgt%7C1&groupBy=season&wxTemperature=&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&position=B&sort=22,1&pageitems=2000000000&pg=0"),

    ("vLHH Stand",
     f"https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=5&splitArrPitch=&autoPt=false&splitTeams=false&statType=player&statgroup=1&startDate={S}-03-01&endDate={S}-11-01&players=&filter=&groupBy=season&wxTemperature=&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&position=P&sort=22,1&pageitems=2000000000&pg=0"),

    ("vLHH Adv",
     f"https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=5&splitArrPitch=&autoPt=false&splitTeams=false&statType=player&statgroup=2&startDate={S}-03-01&endDate={S}-11-01&players=&filter=&groupBy=season&wxTemperature=&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&position=P&sort=22,1&pageitems=2000000000&pg=0"),

    ("vRHH Stand",
     f"https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=6&splitArrPitch=&autoPt=false&splitTeams=false&statType=player&statgroup=1&startDate={S}-03-01&endDate={S}-11-01&players=&filter=&groupBy=season&wxTemperature=&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&position=P&sort=22,1&pageitems=2000000000&pg=0"),

    ("vRHH Adv",
     f"https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=6&splitArrPitch=&autoPt=false&splitTeams=false&statType=player&statgroup=2&startDate={S}-03-01&endDate={S}-11-01&players=&filter=&groupBy=season&wxTemperature=&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&position=P&sort=22,1&pageitems=2000000000&pg=0"),
]


def extract_csv(page, name):
    el = page.query_selector('a.data-export')
    if not el:
        raise RuntimeError("a.data-export not found on page")

    rows = page.query_selector_all('tr')
    print(f"    Table rows visible: {len(rows)}")
    if len(rows) < 2:
        raise RuntimeError("Table appears empty — data may not have loaded")

    # Check if href is already populated (fast path)
    current = page.eval_on_selector("a.data-export", "el => el.href")
    if current and not current.endswith('undefined') and ',' in current:
        csv_content = urllib.parse.unquote(current.split(",", 1)[1])
        if csv_content.strip():
            print(f"    href pre-populated — skipping click")
            return csv_content

    # FanGraphs computes CSV only on a real click. Intercept the download.
    print("    Clicking export button (intercepting download)...")
    try:
        with page.expect_download(timeout=20000) as dl_info:
            page.click('a.data-export')
        dl = dl_info.value
        path = dl.path()
        with open(path, 'r', encoding='utf-8-sig', errors='replace') as f:
            content = f.read()
        if content.strip() and content.strip().lower() != 'undefined':
            print(f"    Download captured: {len(content)} bytes")
            return content
        print(f"    Download content was '{content[:60]}' — trying href fallback")
    except Exception as e:
        print(f"    Download intercept error: {e}")

    # Fallback: maybe the click updated the href without triggering a file download
    time.sleep(2)
    current = page.eval_on_selector("a.data-export", "el => el.href")
    print(f"    href after click: {current[:80]}")
    if current and not current.endswith('undefined') and ',' in current:
        csv_content = urllib.parse.unquote(current.split(",", 1)[1])
        if csv_content.strip():
            return csv_content

    # Take screenshot to help diagnose
    shot = os.path.join(DOWNLOAD_DIR, f"debug_{name.replace(' ', '_')}.png")
    page.screenshot(path=shot)
    print(f"    Screenshot saved: {shot}")
    raise RuntimeError("Could not extract CSV — check screenshot for page state")


def login(p):
    """Open browser for manual login, save session to AUTH_FILE."""
    print("\nFirst-time setup: log in to FanGraphs in the browser window.")
    print("Once logged in, come back here and press Enter.\n")
    browser = p.chromium.launch(headless=False)
    ctx = browser.new_context()
    page = ctx.new_page()
    page.goto("https://www.fangraphs.com/login", timeout=120000, wait_until="domcontentloaded")
    input("Press Enter after you have logged in to FanGraphs...")
    ctx.storage_state(path=AUTH_FILE)
    browser.close()
    print(f"Session saved to {AUTH_FILE}\n")


def run():
    print(f"download_fangraphs.py — season {SEASON}")
    print(f"Saving to: {DOWNLOAD_DIR}\n")

    with sync_playwright() as p:
        if not os.path.exists(AUTH_FILE):
            login(p)

        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(storage_state=AUTH_FILE)
        page = ctx.new_page()
        ok, failed = [], []

        for name, url in PAGES:
            dest = os.path.join(DOWNLOAD_DIR, f"{name}.csv")
            print(f"  [{name}]...")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_load_state("networkidle", timeout=30000)
                csv_content = extract_csv(page, name)
                with open(dest, "w", encoding="utf-8", newline="") as f:
                    f.write(csv_content)
                rows = csv_content.count("\n")
                print(f"    OK — {rows} rows → {name}.csv")
                ok.append(name)
            except Exception as e:
                print(f"    ERROR — {e}")
                failed.append(name)
            time.sleep(2)

        browser.close()

    print(f"\n{'='*50}")
    print(f"  Downloaded: {len(ok)}/{len(PAGES)}")
    if failed:
        print(f"  Failed: {', '.join(failed)}")
        print(f"  If you see auth errors, delete fg_auth.json and re-run to log in again.")
    else:
        print(f"  All done — run: py -3.12 load_fangraphs_excel.py")


if __name__ == "__main__":
    run()
