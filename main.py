from flask import Flask, request
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)

SHEETS_CONFIG = {
    "sheet_id": "191pGup4d902zzD-Qm6d_BYudJ18DWSVuaiUHbZoUvkI",
    "scopes": ["https://www.googleapis.com/auth/spreadsheets"],
    "key_file": "rentmanager-sheets-key.json"
}

FIELDWORK_CONFIG = {
    "base_url": "https://app.fieldworkhq.com/api",
    "endpoint": "/work_orders",
    "api_key": "YOUR_FIELDWORK_API_KEY"
}

def get_sheet_data(sheet, sheet_name, range_name):
    result = sheet.values().get(spreadsheetId=SHEETS_CONFIG["sheet_id"], range=f"{sheet_name}!{range_name}").execute()
    return result.get("values", [])

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S")
    except (ValueError, TypeError):
        return None

def get_recent_unit_ids(unit_info_data):
    now = datetime.now()
    threshold = now - timedelta(hours=24)
    recent_unit_ids = set()
    for row in unit_info_data[1:]:
        if len(row) < 34:
            continue
        unit_id = row[0]
        update_dates = [parse_date(row[i]) for i in [30, 31, 32, 33]]
        if any(date and date >= threshold for date in update_dates):
            recent_unit_ids.add(unit_id)
    return recent_unit_ids

def check_fieldwork_freshair(freshair_data, unit_ids):
    freshair_unit_ids = {row[0] for row in freshair_data[1:] if row and len(row) > 0}
    return {uid: uid in freshair_unit_ids for uid in unit_ids}

def build_fieldwork_payload(unit_row):
    return {
        "unit_id": unit_row[0],
        "property_id": unit_row[2],
        "tenant_name": unit_row[22],
        "is_vacant": unit_row[20] == "true",
        "address": {
            "street1": unit_row[7],
            "street2": unit_row[8],
            "city": unit_row[9],
            "state": unit_row[10],
            "postal_code": unit_row[11]
        }
    }

def call_fieldwork_api(payload):
    headers = {"Authorization": f"Bearer {FIELDWORK_CONFIG['api_key']}", "Content-Type": "application/json"}
    url = f"{FIELDWORK_CONFIG['base_url']}{FIELDWORK_CONFIG['endpoint']}"
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    logger.info(f"Sent to FieldWork HQ: {payload['unit_id']}")
    return response.json()

@app.route("/sync", methods=["POST"])
def sync_to_fieldwork_hq():
    logger.info(f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        creds = service_account.Credentials.from_service_account_file(
            SHEETS_CONFIG["key_file"], scopes=SHEETS_CONFIG["scopes"]
        )
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        unit_info_data = get_sheet_data(sheet, "Unit Info", "A1:AH")
        if not unit_info_data or len(unit_info_data) < 2:
            return {"status": "error", "message": "Unit Info sheet empty"}, 400

        recent_unit_ids = get_recent_unit_ids(unit_info_data)
        if not recent_unit_ids:
            return {"status": "success", "message": "No recent updates"}, 200

        freshair_data = get_sheet_data(sheet, "FieldWork- FreshAir", "A1:A")
        existence_map = check_fieldwork_freshair(freshair_data, recent_unit_ids)

        unit_id_to_row = {row[0]: row for row in unit_info_data[1:] if row[0] in recent_unit_ids}
        synced_count = 0
        for unit_id, exists in existence_map.items():
            if not exists:
                payload = build_fieldwork_payload(unit_id_to_row[unit_id])
                call_fieldwork_api(payload)
                synced_count += 1

        logger.info(f"Completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return {
            "status": "success",
            "message": f"Synced {synced_count} of {len(recent_unit_ids)} units"
        }, 200

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {"status": "error", "message": str(e)}, 500

@app.route("/", methods=["GET"])
def health_check():
    return "Service is running", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
