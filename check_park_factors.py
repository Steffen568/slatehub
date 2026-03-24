import os
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

# Check what columns exist and sample data
res = sb.table('park_factors').select('*').limit(3).execute()
if res.data:
    print("Columns:", list(res.data[0].keys()))
    print()
    for r in res.data:
        print(r)


        