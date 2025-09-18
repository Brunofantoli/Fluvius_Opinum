from zoneinfo import ZoneInfo
import requests
import os
from datetime import datetime, timedelta, time, UTC

def get_timezone_offset_hours(dt):
    """
    Returns the offset in hours between UTC and the timezone of the given datetime.
    dt must be timezone-aware.
    """
    offset = dt.utcoffset()
    return offset.total_seconds() / 3600 if offset is not None else 0

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

    url = "https://apihub.fluvius.be/esco-live/api/v2.0/mandate/energy"
    headers = {
        "Authorization": f"Bearer {fluvius_token}",
        "Ocp-Apim-Subscription-Key": os.getenv("FLUVIUS_SUBSCRIPTION_KEY"),
    }
    params = {
        "eanNumber": ean,
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
        electricity_meters = raw_data["data"]["electricityMeters"]
        if not electricity_meters:
            print("❌ No electricity meters found.")
            return []
    except KeyError:
        print("❌ Expected key 'data.electricityMeters' not found.")
        return []
    formatted_data = []
    brussels_tz = ZoneInfo("Europe/Brussels")
    offset = get_timezone_offset_hours(datetime.now(brussels_tz))
    print(f"Current timezone offset from UTC: {offset} hours")
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
                # Convert UTC timestamp to Brussels time
                try:
                    dt_utc = timestamp.isoformat.replace("Z", "+00:00")
                    dt_brussels = dt_utc.timedelta(hours=offset)
                    local_timestamp = dt_brussels.isoformat()
                except Exception as e:
                    print(f"Timestamp conversion error: {e}")
                    local_timestamp = timestamp
                formatted_data.append({
                    "date": local_timestamp,
                    "value": offtake
                })
    print("The data is sent between these two dates: ", formatted_data[0]["date"],formatted_data[-1]["date"] if formatted_data else "N/A")
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
