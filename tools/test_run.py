from dotenv import load_dotenv
load_dotenv()

from store_profiles import STORE_PROFILES
from core import run_update

df, summary = run_update(
    store="spoofy",
    store_profiles=STORE_PROFILES,
    dry_run=True,
)

print(summary)
