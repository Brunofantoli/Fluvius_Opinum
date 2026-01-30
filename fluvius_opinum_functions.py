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
    """
    Support both v3 formats:
    - headpoint -> physicalMeters[] -> quarterHourlyEnergy[]
    - headpoint (metering-on-headpoint) -> quarterHourlyEnergy[]
    Extract 'offtake.total.value' (or fallback to 'offtake.value') and convert UTC -> Europe/Brussels.
    """
    try:
        headpoint = raw_data["data"]["headpoint"]
    except KeyError:
        print("❌ Expected key 'data.headpoint' not found.")
        return []

    formatted_data = []
    brussels_tz = ZoneInfo("Europe/Brussels")

    def extract_offtake_from_measurements(measurements):
        if not measurements:
            return None
        m = measurements[0]
        # primary path in v3
        offtake = m.get("offtake", {}).get("total", {}).get("value")
        # fallback if structure differs
        if offtake is None:
            offtake = m.get("offtake", {}).get("value")
        return offtake

    # Case A: physicalMeters present (metering-on-meter)
    if "physicalMeters" in headpoint and headpoint.get("physicalMeters"):
        for meter in headpoint.get("physicalMeters", []):
            qlist = meter.get("quarterHourlyEnergy", []) or []
            for entry in qlist:
                timestamp = entry.get("start")
                offtake = extract_offtake_from_measurements(entry.get("measurements", []))
                if timestamp and offtake is not None:
                    try:
                        dt_utc = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                        local_timestamp = timestamp
                    except Exception as e:
                        print(f"Timestamp conversion error: {e}")
                        local_timestamp = timestamp
                    formatted_data.append({"date": local_timestamp, "value": offtake})

    # Case B: quarterHourlyEnergy directly under headpoint (metering-on-headpoint)
    elif "quarterHourlyEnergy" in headpoint and headpoint.get("quarterHourlyEnergy"):
        for entry in headpoint.get("quarterHourlyEnergy", []) or []:
            timestamp = entry.get("start")
            offtake = extract_offtake_from_measurements(entry.get("measurements", []))
            if timestamp and offtake is not None:
                try:
                    dt_utc = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    local_timestamp = timestamp
                except Exception as e:
                    print(f"Timestamp conversion error: {e}")
                    local_timestamp = timestamp
                formatted_data.append({"date": local_timestamp, "value": offtake})

    else:
        print("❌ No quarterHourlyEnergy found in headpoint or physicalMeters.")
        return []

    if formatted_data:
        print("The data is sent between these two dates: ", formatted_data[0]["date"], formatted_data[-1]["date"])
    else:
        print("No data was sent for this period.")

    return [{"variableId": variable_id, "data": formatted_data}]

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


def get_fluvius_short_url(fluvius_token, contract_number, reference_number, flow, dataPeriodFrom="2023-02-01T00:00:00Z"):
    url = "https://apihub.fluvius.be/esco-live/v3/api/shortUrlIdentifier"
    data_services = [
        {
            "dataServiceType": "VH_dag",
            "dataPeriodFrom": dataPeriodFrom
            },
            {
                "dataServiceType": "VH_kwartier_uur",
                "dataPeriodFrom": dataPeriodFrom
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
    if response.status_code == 200:
        result = response.json()
        # Extract the identifier from the nested structure
        identifier = None
        if isinstance(result, dict):
            identifier = result.get("data", {}).get("shortUrlIdentifier")
        if not identifier:
            identifier = str(result)
        print("Short URL response:", identifier)
        full_url = f"https://mijn.fluvius.be/verbruik/dienstverlener?id={identifier}"
        return full_url
    else:
        print("Fluvius API error:", response.text)
        return None