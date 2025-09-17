"""
monthly_update.py

This script updates all data for the previous month from Fluvius and sends it to Opinum.
Run this at the beginning of each month to ensure complete data.
"""

from dotenv import load_dotenv
from datetime import datetime, timedelta, UTC
from fluvius_opinum_functions import (
    get_fluvius_token,
    get_opinum_token,
    get_fluvius_data,
    prepare_data,
    send_to_opinum
)

load_dotenv()
variable_id = 7185058  # Replace with actual Opinum variableId
ean = "541448820055391175"  # Replace with actual EAN number

def get_previous_month_date_range():
    today = datetime.now(UTC).date()
    first_day_this_month = today.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)
    return first_day_prev_month, last_day_prev_month

def main():
    first_day, last_day = get_previous_month_date_range()
    print(f"Updating data from {first_day} to {last_day}")
    from_date = f"{first_day.isoformat()}T00:00:00Z"
    to_date = f"{(last_day + timedelta(days=1)).isoformat()}T00:00:00Z"
    opinum_token = get_opinum_token()
    fluvius_token = get_fluvius_token()
    raw_data = get_fluvius_data(fluvius_token, ean, from_date, to_date)
    if raw_data:
        prepared = prepare_data(raw_data, variable_id)
        send_to_opinum(prepared, opinum_token)
    print("Monthly update complete.")

if __name__ == "__main__":
    main()
