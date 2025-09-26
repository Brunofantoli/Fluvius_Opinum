from zoneinfo import ZoneInfo
import requests
import os
from datetime import datetime, timedelta, time, UTC

def get_fluvius_token():
    client_id = os.getenv("FLUVIUS_CLIENT_ID")
    tenant_id = os.getenv("FLUVIUS_TENANT_ID")
    cert_thumbprint = os.getenv("FLUVIUS_CERT_THUMBPRINT")
    private_key_path = os.getenv("FLUVIUS_KEY_PATH")
    public_cert_path = os.getenv("FLUVIUS_CER_PATH")
    scope = os.getenv("FLUVIUS_SCOPE")
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    with open(private_key_path, 'r') as pkf: # type: ignore
        private_key = pkf.read()
    with open(public_cert_path, 'r') as cf: # type: ignore
        public_cert = cf.read()
    from msal import ConfidentialClientApplication
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

def get_fluvius_data(fluvius_token, ean, start_date_local, end_date_local):
    """
    start_date_local, end_date_local: datetime.date objects in Brussels local time
    """
    from_date = start_date_local
    to_date = end_date_local

    url = "https://apihub.fluvius.be/esco-live/v3/api/mandate/energy"
    headers = {
        "Authorization": f"Bearer {fluvius_token}",
        "Ocp-Apim-Subscription-Key": os.getenv("FLUVIUS_SUBSCRIPTION_KEY"),
    }
    params = {
        "ean": ean,
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

def prepare_data(raw_data, variable_id):
    try:
        physical_meters = raw_data["data"]["headpoint"]["physicalMeters"]
        if not physical_meters:
            print("❌ No physical meters found.")
            return []
    except KeyError:
        print("❌ Expected key 'data.headpoint.physicalMeters' not found.")
        return []
    formatted_data = []
    brussels_tz = ZoneInfo("Europe/Brussels")
    for meter in physical_meters:
        quarter_hourly_data = meter.get("quarterHourlyEnergy", [])
        for entry in quarter_hourly_data:
            timestamp = entry.get("start")
            measurements = entry.get("measurements", [])
            if not timestamp or not measurements:
                continue
            # v3: measurements is a list, but we expect only one per entry
            measurement = measurements[0]
            offtake = (
                measurement.get("offtake", {})
                .get("total", {})
                .get("value")
            )
            if offtake is not None:
                # Convert UTC timestamp to UTC +1
                try:
                    dt_utc = datetime.fromisoformat(timestamp)
                    local_timestamp = (dt_utc + timedelta(hours=1)).isoformat().replace("+00:00","Z") #Opinum expects timestamps in UTC+1
                except Exception as e:
                    print(f"Timestamp conversion error: {e}")
                    local_timestamp = timestamp
                formatted_data.append({
                    "date": local_timestamp,
                    "value": offtake
                })
    if formatted_data:
        print("The data is sent between these two dates: ", formatted_data[0]["date"], formatted_data[-1]["date"])
    else:
        print("No data was sent for this period.")
    return [{
        "variableId": variable_id,
        "data": formatted_data
    }]

def send_to_opinum(data, opinum_token):
    url = "https://push.opinum.com/api/data"
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


def get_fluvius_short_url(fluvius_token, contract_number, reference_number, flow, data_services):
    url = "https://apihub.fluvius.be/esco-live/v3/api/shortUrlIdentifier"
    data_services = [
        {
            "dataServiceType": "VH_dag",
            "dataPeriodFrom": "2023-01-01T23:00:00Z"
            },
            {
                "dataServiceType": "VH_kwartier_uur",
                "dataPeriodFrom": "2023-01-01T00:00:00Z"
                }
                ]
    headers = {
        "Authorization": f"Bearer {fluvius_token}",
        "Ocp-Apim-Subscription-Key": os.getenv("FLUVIUS_SUBSCRIPTION_KEY"),
        "Content-Type": "application/json"
    }
    body = {
        "dataAccessContractNumber": contract_number,
        "referenceNumber": reference_number,
        "flow": flow,
        "dataServices": data_services
    }
    response = requests.post(url, headers=headers, json=body)
    print(response)
    if response.status_code == 200:
        result = response.json()
        print("Short URL response:", result)
        # Assume the API response contains the identifier as a string or in a field
        # If result is a dict, try to extract the identifier
        if isinstance(result, dict):
            identifier = result.get("identifier", str(result))
        else:
            identifier = str(result)
        full_url = f"https://mijn.fluvius.be/verbruik/dienstverlener?id={identifier}"
        return full_url
    else:
        print("Fluvius API error:", response.text)
        return None