from supabase import create_client
from dotenv import load_dotenv
import os

load_dotenv()
sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

result = sb.from_('batter_stats') \
    .select('player_id, season, wrc_plus, woba') \
    .eq('player_id', 656775) \
    .execute()

print("Seasons found for Cedric Mullins:")
for row in result.data:
    print(f"  Season {row['season']}: wRC+ = {row['wrc_plus']}, wOBA = {row['woba']}")

print(f"\nTotal rows: {len(result.data)}")
