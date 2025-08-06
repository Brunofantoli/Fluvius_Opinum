import requests
import os
from dotenv import load_dotenv
from msal import ConfidentialClientApplication


# Load API keys from .env
load_dotenv()
from_date = "2025-08-01T23:00:00Z"
to_date = "2025-08-04T23:00:00Z"



FLUVIUS_API_KEY = os.getenv("FLUVIUS_API_KEY")

## Fluvius token acquisition
def get_fluvius_token():
    client_id = os.getenv("FLUVIUS_CLIENT_ID")
    tenant_id = os.getenv("FLUVIUS_TENANT_ID")
    cert_thumbprint = os.getenv("FLUVIUS_CERT_THUMBPRINT")
    private_key_path = os.getenv("FLUVIUS_KEY_PATH")     # .key file
    public_cert_path = os.getenv("FLUVIUS_CER_PATH")     # .cer file
    scope = os.getenv("FLUVIUS_SCOPE")

    authority = f"https://login.microsoftonline.com/{tenant_id}"

    with open(private_key_path, 'r') as pkf: # type: ignore
        private_key = pkf.read()
    with open(public_cert_path, 'r') as cf: # type: ignore
        public_cert = cf.read()

    app = ConfidentialClientApplication(
        client_id=client_id,
        authority=authority,
        client_credential={
            "private_key": private_key,
            "thumbprint": cert_thumbprint,
            "public_certificate": public_cert
        }
    )

    result = app.acquire_token_for_client(scopes=[scope])
    
    if "access_token" in result: # type: ignore
        print("Fluvius token acquired.")
        return result["access_token"] # type: ignore
    else:
        print("Fluvius token request failed:", result.get("error_description")) # type: ignore
        return None

## Opinum Token Acquisition
def get_opinum_token():
    url = os.getenv("OPINUM_TOKEN_URL")
    data = {
        "grant_type": "password",
        "client_id": os.getenv("OPINUM_CLIENT_ID"),
        "client_secret": os.getenv("OPINUM_CLIENT_SECRET"),
        "username": os.getenv("OPINUM_USERNAME"),
        "password": os.getenv("OPINUM_PASSWORD"),
        "scope": os.getenv("OPINUM_SCOPE")
    }

    response = requests.post(url, data=data) # type: ignore

    if response.status_code == 200:
        token = response.json().get("access_token")
        print("Opinum token acquired.")
        return token
    else:
        print("Token request failed:", response.text)
        return None
    
opinum_token = get_opinum_token()
fluvius_token = get_fluvius_token()

if opinum_token:
    headers = {
        "Authorization": f"Bearer {opinum_token}",
        "Content-Type": "application/json"
    }
    # You can now send data using this token in the headers

# --- Step 1: Get data from Fluvius ---
def get_fluvius_data():
    url = "https://apihub.fluvius.be/esco-live/api/v2.0/mandate/energy"
    headers = {
        "Authorization": f"Bearer {fluvius_token}",
        "Ocp-Apim-Subscription-Key": os.getenv("FLUVIUS_SUBSCRIPTION_KEY"),
    }
    params = {
        "eanNumber": "541448820055391175",  
        "PeriodType": "readTime",
        "granularity": "hourly_quarterhourly",
        "from": from_date,
        "to": to_date
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        print("Fluvius data retrieved.")
        return data
    else:
        print("Fluvius API error:", response.text)
        return None

# --- Step 2: Format data (if needed) ---
def prepare_data(raw_data):
    """
    Transforms Fluvius quarter-hourly electricity data into Opinum format.
    """
    variable_id = 7185058  # Replace with actual Opinum variableId

    # Navigate safely into the data structure
    try:
        electricity_meters = raw_data["data"]["electricityMeters"]
        if not electricity_meters:
            print("❌ No electricity meters found.")
            return []
    except KeyError:
        print("❌ Expected key 'data.electricityMeters' not found.")
        return []

    formatted_data = []

    for meter in electricity_meters:
        quarter_hourly_data = meter.get("quarterHourlyEnergy", [])
        for entry in quarter_hourly_data:
            timestamp = entry.get("timestampStart")
            measurements = entry.get("measurement", [])

            if not timestamp or not measurements:
                continue

            measurement = measurements[0]
            offtake = measurement.get("offtakeValue")

            if offtake is not None:
                formatted_data.append({
                    "date": timestamp,
                    "value": offtake
                })

    return [{
        "variableId": variable_id,
        "data": formatted_data
    }]


# --- Step 3: Send to Opinum ---
def send_to_opinum(data):
    url = "https://push.opinum.com/api/data"  # Replace with real Opinum endpoint
    headers = {
        "Authorization": f"Bearer {opinum_token}",
        "Content-Type": "application/json",
        "scope": "push-data"
    }
    body = data

    response = requests.post(url, headers=headers, json=body)

    if response.status_code == 200 or response.status_code == 201:
        print("Data sent to Opinum successfully.")
    else:
        print("Opinum API error:", response.text)

# --- Main Function ---
def main():
    raw_data = get_fluvius_data()
    if raw_data:
        prepared = prepare_data(raw_data)
        send_to_opinum(prepared)

if __name__ == "__main__":
    main()