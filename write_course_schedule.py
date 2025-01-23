from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from google_auth_httplib2 import AuthorizedHttp
import httplib2
from datetime import datetime, timedelta

def initialize_firebase():
    firebase_cred = credentials.Certificate("firebase-adminsdk.json")
    initialize_app(firebase_cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

def get_google_sheets_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    authorized_http = AuthorizedHttp(google_creds, http=httplib2.Http(timeout=60))
    return build('sheets', 'v4', cache_discovery=False, http=authorized_http)

def get_firebase_data(ref_path):
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Error fetching Firebase data: {e}")
        return None

def get_course_ids():
    courses = get_firebase_data("Courses/course_id")
    if not courses:
        return []
    return [i for i, course in enumerate(courses) if course]

def get_student_indices(course_id):
    enrollment_data = get_firebase_data(f"Students/enrollment/course_id/{course_id}")
    if not enrollment_data or "student_index" not in enrollment_data:
        return []
    return enrollment_data["student_index"].split(", ")

def get_student_names(student_indices):
    student_names = []
    for student_index in student_indices:
        student_name = get_firebase_data(f"Students/student_info/{student_index}/student_name")
        if student_name:
            student_names.append(student_name)
    return student_names

def get_sheet_id(course_id):
    sheet_data = get_firebase_data(f"Courses/course_id/{course_id}/course_sheet_id")
    if not sheet_data:
        return None

    # Fetch the internal sheet ID using the Google Sheets API
    sheets_service = get_google_sheets_service()
    spreadsheet_id = sheet_data  # Assuming this is the spreadsheet ID
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get("sheets", [])
        if sheets:
            # Return the first sheet's ID as a default behavior
            return sheets[0]["properties"]["sheetId"]
    except Exception as e:
        print(f"Error fetching sheet ID: {e}")
    return None

def prepare_update_requests(sheet_id, student_names, month, year=2025):
    requests = []
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 35},
            "fields": "pixelSize"
        }
    })
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},
            "properties": {"pixelSize": 100},
            "fields": "pixelSize"
        }
    })
    requests.append({"updateCells": {
        "rows": [{"values": [
            {"userEnteredValue": {"stringValue": "出席番号"}},
            {"userEnteredValue": {"stringValue": "学生名"}}
        ]}],
        "start": {"sheetId": sheet_id, "rowIndex": 0, "columnIndex": 0},
        "fields": "userEnteredValue"
    }})
    for i, name in enumerate(student_names):
        requests.append({"updateCells": {
            "rows": [{"values": [
                {"userEnteredValue": {"numberValue": i + 1}},
                {"userEnteredValue": {"stringValue": name}}
            ]}],
            "start": {"sheetId": sheet_id, "rowIndex": i + 1, "columnIndex": 0},
            "fields": "userEnteredValue"
        }})
    current_date = datetime(year, month, 1)
    while current_date.month == month:
        col_index = current_date.day + 1
        requests.append({"updateCells": {
            "rows": [{"values": [
                {"userEnteredValue": {"stringValue": current_date.strftime("%m/%d")}}
            ]}],
            "start": {"sheetId": sheet_id, "rowIndex": 0, "columnIndex": col_index},
            "fields": "userEnteredValue"
        }})
        current_date += timedelta(days=1)

    # Set weekend color
    current_date = datetime(year, month, 1)
    while current_date.month == month:
        if current_date.weekday() in [5, 6]:  # Saturday or Sunday
            col_index = current_date.day + 1
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": len(student_names) + 1,
                        "startColumnIndex": col_index,
                        "endColumnIndex": col_index + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 1.0, "green": 0.9, "blue": 0.9}
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
        current_date += timedelta(days=1)

    return requests

def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()
    course_ids = get_course_ids()
    for course_id in course_ids:
        sheet_id = get_sheet_id(course_id)
        if not sheet_id:
            continue
        student_indices = get_student_indices(course_id)
        student_names = get_student_names(student_indices)
        for month in range(1, 13):
            requests = prepare_update_requests(sheet_id, student_names, month)
            if requests:
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={"requests": requests}
                ).execute()

if __name__ == "__main__":
    main()
