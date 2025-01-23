from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_transport.requests import Request
from google_auth_httplib2 import AuthorizedHttp
import httplib2
import time
import socket
from datetime import datetime, timedelta

def initialize_firebase():
    print("Initializing Firebase...")
    firebase_cred = credentials.Certificate("firebase-adminsdk.json")
    initialize_app(firebase_cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })
    print("Firebase initialized.")

def get_google_sheets_service():
    print("Initializing Google Sheets service...")
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    authorized_http = AuthorizedHttp(google_creds, http=httplib2.Http(timeout=60))
    print("Google Sheets service initialized.")
    return build('sheets', 'v4', cache_discovery=False, http=authorized_http)

def get_firebase_data(ref_path):
    print(f"Fetching data from Firebase path: {ref_path}")
    try:
        data = db.reference(ref_path).get()
        print(f"Data fetched: {data}")
        return data
    except Exception as e:
        print(f"Error fetching data from Firebase: {e}")
        return None

def execute_with_retry(request, retries=3, delay=5):
    for attempt in range(retries):
        try:
            print(f"Executing request, attempt {attempt + 1}/{retries}...")
            return request.execute()
        except (HttpError, socket.timeout) as e:
            print(f"Request failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise

def create_cell_update_request(sheet_id, row_index, column_index, value):
    print(f"Creating cell update request for sheet ID {sheet_id}, row {row_index}, column {column_index}, value {value}")
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": str(value)}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

def create_sheet_request(sheet_title):
    print(f"Creating request to add sheet titled {sheet_title}")
    return {
        "addSheet": {
            "properties": {"title": sheet_title}
        }
    }

def generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title):
    print(f"Generating unique sheet title based on base title {base_title}")
    existing_sheets = execute_with_retry(
        sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id)
    ).get("sheets", [])
    sheet_titles = [sheet["properties"]["title"] for sheet in existing_sheets]
    title = base_title
    counter = 1
    while title in sheet_titles:
        title = f"{base_title} ({counter})"
        counter += 1
    print(f"Generated unique title: {title}")
    return title

def prepare_update_requests(sheets_service, spreadsheet_id, sheet_title, student_names):
    print(f"Preparing update requests for spreadsheet ID {spreadsheet_id}, sheet title {sheet_title}")
    sheet_metadata = execute_with_retry(
        sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id)
    )
    sheet_id = None
    for sheet in sheet_metadata.get("sheets", []):
        if sheet["properties"]["title"] == sheet_title:
            sheet_id = sheet["properties"]["sheetId"]
            break

    if sheet_id is None:
        print(f"Sheet {sheet_title} does not exist. Adding new sheet.")
        add_sheet_request = create_sheet_request(sheet_title)
        execute_with_retry(
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [add_sheet_request]}
            )
        )
        sheet_metadata = execute_with_retry(
            sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id)
        )
        for sheet in sheet_metadata.get("sheets", []):
            if sheet["properties"]["title"] == sheet_title:
                sheet_id = sheet["properties"]["sheetId"]
                break

    if sheet_id is None:
        print(f"Failed to create or find sheet {sheet_title}.")
        return []

    requests = []
    for i, name in enumerate(student_names):
        requests.append(create_cell_update_request(sheet_id, i + 1, 0, name))

    print("Update requests prepared.")
    return requests

def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    class_indices = get_firebase_data('Class/class_index')
    if not class_indices or not isinstance(class_indices, dict):
        print("Class indices not found or invalid.")
        return

    for class_index, class_data in class_indices.items():
        spreadsheet_id = class_data.get("class_sheet_id")
        if not spreadsheet_id:
            print(f"Spreadsheet ID not found for class {class_index}.")
            continue

        student_names = get_firebase_data(f"Students/enrollment/course_id/{class_index}/student_index")
        if not student_names:
            print(f"No student names found for class {class_index}.")
            continue

        student_names = student_names.split(",")
        sheet_title = generate_unique_sheet_title(sheets_service, spreadsheet_id, f"Class_{class_index}")
        requests = prepare_update_requests(sheets_service, spreadsheet_id, sheet_title, student_names)
        if requests:
            execute_with_retry(
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": requests}
                )
            )
            print(f"Spreadsheet {spreadsheet_id} updated successfully for class {class_index}.")

if __name__ == "__main__":
    main()
