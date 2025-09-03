# constants.py
API_VERSION = "2024-01"
BATCH_SIZE_DEFAULT = 50
SLEEP_BETWEEN_CALLS = 0.3
RETRY_429_MAX = 5
STOCK_CSV_PATH = "Stock_Update.csv"

# Size thresholds in MB for split files
SIZE_WARN_MB = 40
SIZE_ALERT_MB = 50
SIZE_HARD_MB = 60


size_map = {
    "XS": "XS", "Small": "S", "Medium": "M", "Large": "L",
    "XL": "XL", "2XL": "2XL", "3XL": "3XL", "4XL": "4XL", "5XL": "5XL"
}
colour_map = {
    "White": "WHIT", "Black": "BLAC", "VintageBlue": "VBLU",
    "CityRed": "CIRD", "HibiskusPink": "HIPI", "DarkGrey": "DKGY",
    "IntenseBlue": "INTB", "RetroGreen": "REGR", "Kelly": "KELL",
    "Yellow": "YELL", "Orange": "ORAN", "Pink": "PINK"
}

# Which Google Sheet IDs feed each store (first tab is used)
SHEET_SOURCES = {
    "paddy": [
        "1OFD367c_04TfDTaoLpGdy5qeGCOkivMf4xA8r0CPbCc",
        "1Ra0sg9xH8r_nDKLwUANhqb1SCNsepKHKswcnXYhlz4Q",
    ],
    "spoofy": [
        "1TxokufF-Ct9s8P6zjc6quIJrxbmJudYfdJ7R6ij8Z6o",
        "1XiXSIPAORIzK_PT38Kaf3_JySCqvLrPFy4ag1EssGB8",
    ],
}
