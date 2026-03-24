import requests, io, pandas as pd

HDRS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Baseball Savant pitcher splits vs LHH and RHH
for hand, label in [('L','vsLHH'), ('R','vsRHH')]:
    url = (
        f"https://baseballsavant.mlb.com/leaderboard/custom"
        f"?year=2025&type=pitcher&filter=&min=1"
        f"&selections=p_total_pa,p_strikeout,p_walk,p_home_run,"
        f"batting_avg,slg_percent,on_base_percent,woba,"
        f"xwoba,xba,xslg,exit_velocity_avg,hard_hit_percent,"
        f"barrel_batted_rate,whiff_percent&csv=true"
        f"&split_batter_hand={hand}"
    )
    try:
        r = requests.get(url, headers=HDRS, timeout=20)
        df = pd.read_csv(io.StringIO(r.text))
        print(f"{label}: {len(df)} rows, cols: {list(df.columns)}")
        # Find Bubic or any pitcher
        sample = df.iloc[0] if len(df) else None
        if sample is not None:
            print(f"  Sample: {sample.get('last_name, first_name')} woba={sample.get('woba')} xwoba={sample.get('xwoba')}")
    except Exception as e:
        print(f"{label} error: {e}")
        