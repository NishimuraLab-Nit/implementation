from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google_auth_httplib2 import AuthorizedHttp
import httplib2
import time
import socket
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
        print(f"Firebase data retrieval error: {e}")
        return None

def execute_with_retry(request, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return request.execute()
        except (HttpError, socket.timeout) as e:
            print(f"Request failed ({attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise

def get_sheet_id(course_id):
    course_data = get_firebase_data(f'Courses/course_id/{course_id}/course_sheet_id')
    if not course_data:
        print(f"Course sheet ID not found for course_id: {course_id}")
        return None
    return course_data

def get_student_names(course_id):
    enrollment_data = get_firebase_data(f'Students/enrollment/course_id/{course_id}/student_index')
    if not enrollment_data:
        print(f"No students enrolled in course_id: {course_id}")
        return []

    student_indices = [index.strip() for index in enrollment_data.split(",")]
    student_names = []

    for student_index in student_indices:
        student_info = get_firebase_data(f'Students/student_info/student_index/{student_index}/student_name')
        if student_info:
            student_names.append(student_info)
        else:
            print(f"Student name not found for student_index: {student_index}")

    return student_names

def create_sheet_and_update_data(sheets_service, spreadsheet_id, student_names, month):
    year = datetime.now().year
    sheet_title = f"{year}-{str(month).zfill(2)}"

    # Create a new sheet
    requests = [{
        "addSheet": {
            "properties": {
                "title": sheet_title
            }
        }
    }]

    response = execute_with_retry(
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        )
    )

    new_sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']

    # Update the sheet with student names and attendance numbers
    requests = []
    requests.append({
        "updateCells": {
            "rows": [
                {
                    "values": [
                        {"userEnteredValue": {"stringValue": "AN"}},
                        {"userEnteredValue": {"stringValue": "Student Name"}}
                    ]
                }
            ],
            "start": {"sheetId": new_sheet_id, "rowIndex": 0, "columnIndex": 0},
            "fields": "userEnteredValue"
        }
    })

    for i, student_name in enumerate(student_names):
        requests.append({
            "updateCells": {
                "rows": [
                    {
                        "values": [
                            {"userEnteredValue": {"numberValue": i + 1}},
                            {"userEnteredValue": {"stringValue": student_name}}
                        ]
                    }
                ],
                "start": {"sheetId": new_sheet_id, "rowIndex": i + 1, "columnIndex": 0},
                "fields": "userEnteredValue"
            }
        })

    execute_with_retry(
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        )
    )
    print(f"Sheet {sheet_title} created and updated successfully.")

def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    class_indices = get_firebase_data('Class/class_index')
    if not class_indices:
        print("No classes found in Firebase.")
        return

    for class_key, class_data in class_indices.items():
        course_ids = [int(cid.strip()) for cid in class_data.get("course_id", "").split(",")]
        spreadsheet_id = class_data.get("class_sheet_id")

        for course_id in course_ids:
            sheet_id = get_sheet_id(course_id)
            if not sheet_id:
                continue

            student_names = get_student_names(course_id)
            if not student_names:
                continue

            for month in range(1, 13):
                print(f"Processing sheet for month: {month}")
                create_sheet_and_update_data(sheets_service, spreadsheet_id, student_names, month)

if __name__ == "__main__":
    main()
