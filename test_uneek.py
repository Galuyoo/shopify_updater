import os
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(ROOT, ".env")

print("CWD:", os.getcwd())
print("Looking for .env at:", ENV_PATH, "exists:", os.path.exists(ENV_PATH))

load_dotenv(ENV_PATH, override=True)

print("UNEEK_USER loaded:", bool(os.getenv("UNEEK_USER")))
print("UNEEK_PASS loaded:", bool(os.getenv("UNEEK_PASS")))

from uneek import get_stock

df = get_stock()
print(df.head())
print("rows:", len(df), "cols:", list(df.columns))
