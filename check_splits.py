import cloudscraper, io, pandas as pd

scraper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
)

for split_id, label in [(1,'vsLHP'), (2,'vsRHP')]:
    url = (f"https://www.fangraphs.com/api/leaders/splits/batters"
           f"?season=2025&splitTeams=false&statType=player&statgroup=2"
           f"&startDate=2025-03-01&endDate=2025-11-01&players=&splitArr={split_id}"
           f"&position=B&autoPt=false&minPA=1&sort=WAR,1&pg=0")
    try:
        r = scraper.get(url, timeout=20)
        print(f"{label}: status={r.status_code} len={len(r.text)}")
        if r.status_code == 200 and r.text.strip().startswith('['):
            data = r.json()
            print(f"  rows: {len(data)}")
            if data:
                print(f"  keys: {list(data[0].keys())[:15]}")
                # Find Soto
                soto = [p for p in data if 'soto' in str(p).lower()]
                if soto:
                    print(f"  Soto: {soto[0]}")
        else:
            print(f"  preview: {r.text[:200]}")
    except Exception as e:
        print(f"{label} error: {e}")

        