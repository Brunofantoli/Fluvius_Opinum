"""
custom_range_update.py

This script updates all data for a user-specified date range from Fluvius and sends it to Opinum.
Usage:
    python custom_range_update.py YYYY-MM-DD YYYY-MM-DD
"""

import sys
from dotenv import load_dotenv
from datetime import datetime, timedelta, UTC
from fluvius_opinum_functions import (
    get_fluvius_token,
    get_opinum_token,
    get_fluvius_data,
    prepare_data,
    send_to_opinum
)
from config import EAN_VARIABLE_PAIRS

load_dotenv()

def main():
    if len(sys.argv) != 3:
        print("Usage: python custom_range_update.py <start_date> <end_date>")
        print("Example: python custom_range_update.py 2025-08-01 2025-08-31")
        sys.exit(1)
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        print("Dates must be in YYYY-MM-DD format.")
        sys.exit(1)
    print(f"Updating data from {start_dt} to {end_dt}")
    from_date = f"{start_dt.isoformat()}T00:00:00Z"
    to_date = f"{(end_dt + timedelta(days=1)).isoformat()}T00:00:00Z"
    opinum_token = get_opinum_token()
    fluvius_token = get_fluvius_token()
    for ean, variable_id in EAN_VARIABLE_PAIRS:
        print(f"Processing EAN: {ean}, Variable ID: {variable_id}")
        raw_data = get_fluvius_data(fluvius_token, ean, from_date, to_date)
        if raw_data:
            prepared = prepare_data(raw_data, variable_id)
            send_to_opinum(prepared, opinum_token)
    print("Custom range update complete.")

if __name__ == "__main__":
    main()
