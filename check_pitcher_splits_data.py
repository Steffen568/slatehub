import os
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

# Two entries for same pitcher — one MLB ID, one FG ID
# Check which one has actual stats
res = sb.table('pitcher_stats').select('*').in_('full_name', ['Cole Ragans', 'MacKenzie Gore']).execute()
for r in res.data:
    print(f"  pid={r['player_id']} name={r['full_name']} era={r.get('era')} k_pct={r.get('k_pct')} ip={r.get('ip')}")

print()
# Check pitcher_splits IDs — are they FG IDs or MLB IDs?
print("Splits for Cole Ragans FG id 21846:")
res2 = sb.table('pitcher_splits').select('*').eq('player_id', 21846).execute()
for r in res2.data:
    print(f"  pid={r['player_id']} split={r['split']} era={r['era']} woba={r['woba']}")
    