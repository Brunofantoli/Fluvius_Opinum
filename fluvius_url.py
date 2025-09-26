from dotenv import load_dotenv
from fluvius_opinum_functions import (
    get_fluvius_token,
    get_fluvius_short_url
)
import sys
load_dotenv()


fluvius_token = get_fluvius_token()

def main():
    contract_number = sys.argv[1]
    reference_number = sys.argv[2]
    flow = sys.argv[3]
    data_services = sys.argv[4]

    fluvius_url = get_fluvius_short_url(fluvius_token, contract_number, reference_number, flow, data_services)
    print("The Fluvius url is the following:", fluvius_url)

if __name__ == "__main__":
    main()