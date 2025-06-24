import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent / "backend" / "src"))

from db.supabase_client import supabase

res = supabase.table("movies").select("*").limit(3).execute()
print(res.data)
