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

from config import EAN_VARIABLE_PAIRS

# Get yesterday’s start and end (UTC or local based on Fluvius needs)
yesterday = datetime.now(UTC).date() - timedelta(days=1)
start_date = (yesterday - timedelta(days=1)).isoformat()
end_date = (yesterday + timedelta(days=1)).isoformat()
print(f"Updating data from {start_date} to {end_date}")

from_date = f"{start_date}T00:00:00Z"
to_date = f"{end_date}T00:00:00Z"

opinum_token = get_opinum_token()
fluvius_token = get_fluvius_token()

def main():
    for ean, variable_id in EAN_VARIABLE_PAIRS:
        print(f"Processing EAN: {ean}, Variable ID: {variable_id}")
        raw_data = get_fluvius_data(fluvius_token, ean, from_date, to_date)
        if raw_data:
            prepared = prepare_data(raw_data, variable_id)
            send_to_opinum(prepared, opinum_token)
    print("Daily update complete.")

if __name__ == "__main__":
    main()