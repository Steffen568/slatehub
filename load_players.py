import pandas as pd
import requests
from supabase import create_client
from dotenv import load_dotenv
import os
from io import StringIO
import math

# ── Load credentials from your .env file
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: Missing credentials. Check your .env file.")
    exit()

# ── Connect to Supabase
print("Connecting to Supabase...")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
print("Connected.")

# ── Download all 16 Chadwick Register files
BASE_URL = "https://raw.githubusercontent.com/chadwickbureau/register/refs/heads/master/data/people-{}.csv"
FILES = ['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f']

print("Downloading Chadwick Register (16 files)...")
all_frames = []

for f in FILES:
    url = BASE_URL.format(f)
    print(f"  Fetching people-{f}.csv...", end=" ")
    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            df_part = pd.read_csv(StringIO(response.text), low_memory=False)
            all_frames.append(df_part)
            print(f"OK ({len(df_part):,} rows)")
        else:
            print(f"SKIPPED (status {response.status_code})")
    except Exception as e:
        print(f"ERROR: {e}")

print(f"\nCombining {len(all_frames)} files...")
df = pd.concat(all_frames, ignore_index=True)

# ── Keep only needed columns
df = df[['key_mlbam','key_fangraphs','key_bbref',
         'name_first','name_last','birth_year']].copy()

# Drop rows with no MLBAM ID
df = df.dropna(subset=['key_mlbam'])
df['key_mlbam'] = df['key_mlbam'].astype(int)

# Build full name
import unicodedata

def normalize_name(name):
    if not name or not isinstance(name, str):
        return ''
    # Strip accent marks — converts é→e, ñ→n, ü→u etc.
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
    return ascii_name.strip().lower()

# Build full name and normalized version
df['full_name'] = (df['name_first'].fillna('') + ' ' + df['name_last'].fillna('')).str.strip()
df['name_normalized'] = df['full_name'].apply(normalize_name)

print(f"Players with MLBAM IDs: {len(df):,}")

# ── Helper function to make a single value JSON-safe
def clean(val):
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, float) and math.isinf(val):
        return None
    return val

# ── Convert dataframe to clean list of dicts
print("Cleaning data for upload...")
raw_rows = df.rename(columns={
    'key_mlbam':       'mlbam_id',
    'key_fangraphs':   'fangraphs_id',
    'key_bbref':       'bbref_id',
    'name_first':      'first_name',
    'name_last':       'last_name',
    'name_normalized': 'name_normalized',
}).to_dict(orient='records')

# Apply clean() to every value in every row
rows = []
for row in raw_rows:
    cleaned_row = {}
    for key, val in row.items():
        cleaned_val = clean(val)
        # Convert fangraphs_id float like 15640.0 → "15640"
        if key == 'fangraphs_id' and cleaned_val is not None:
            cleaned_val = str(int(float(cleaned_val)))
        # Convert birth_year float → int
        if key == 'birth_year' and cleaned_val is not None:
            cleaned_val = int(float(cleaned_val))
        cleaned_row[key] = cleaned_val
    rows.append(cleaned_row)

print(f"Uploading {len(rows):,} rows in batches of 500...")
success = 0
errors  = 0
BATCH_SIZE = 500

for i in range(0, len(rows), BATCH_SIZE):
    batch = rows[i:i + BATCH_SIZE]
    try:
        supabase.table('players').upsert(batch).execute()
        success += len(batch)
        print(f"  Uploaded {min(i + BATCH_SIZE, len(rows)):,} / {len(rows):,}")
    except Exception as e:
        errors += len(batch)
        print(f"  ERROR on batch {i//BATCH_SIZE + 1}: {e}")
        break  # stop on first error so we can read it

print(f"\nDone. {success:,} rows uploaded, {errors} errors.")
if errors == 0:
    print("Your players table is ready.")

