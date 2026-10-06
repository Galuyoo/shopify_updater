from core import build_headers, audit_mapping_coverage
from store_profiles import STORE_PROFILES
from constants import API_VERSION

store_key = "spoofy"
profile = STORE_PROFILES[store_key]

endpoint = f"https://{profile['SHOP_URL']}/admin/api/{API_VERSION}/graphql.json"
headers = build_headers(profile["ACCESS_TOKEN"])
map_csv = profile["MAP_CSV"]

res = audit_mapping_coverage(endpoint, headers, map_csv, product_types=None, pages=5, progress=print)
print(res)
