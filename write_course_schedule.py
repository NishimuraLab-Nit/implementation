from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
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

def get_all_course_ids():
    print("Fetching all course IDs...")
    courses = get_firebase_data("Courses/course_id")
    if not courses or not isinstance(courses, list):
        print("No course IDs found or invalid data format.")
        return []
    course_ids = [i for i in range(1, len(courses)) if courses[i]]
    print(f"Course IDs fetched: {course_ids}")
    return course_ids

def get_sheet_metadata(service, spreadsheet_id):
    print(f"Fetching sheet metadata for spreadsheet ID: {spreadsheet_id}")
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get("sheets", [])
        sheet_metadata = {sheet["properties"]["title"]: sheet["properties"]["sheetId"] for sheet in sheets}
        print(f"Sheet metadata fetched: {sheet_metadata}")
        return sheet_metadata
    except HttpError as e:
        print(f"Error fetching sheet metadata: {e}")
        return {}

def get_sheet_id(course_id):
    print(f"Fetching sheet ID for course ID: {course_id}")
    course_data = get_firebase_data(f"Courses/course_id/{course_id}")
    if course_data and "course_sheet_id" in course_data:
        print(f"Found sheet ID: {course_data['course_sheet_id']}")
        return course_data["course_sheet_id"]
    print(f"Sheet ID not found for course ID {course_id}.")
    return None

def get_student_names(course_id):
    print(f"Fetching student names for course ID: {course_id}")
    enrollment_data = get_firebase_data(f"Students/enrollment/course_id/{course_id}")
    if not enrollment_data or "student_index" not in enrollment_data:
        print(f"No student index found for course ID {course_id}.")
        return []

    student_indices = enrollment_data["student_index"].split(",")
    student_names = []

    for index in student_indices:
        print(f"Fetching student info for index: {index.strip()}")
        student_info = get_firebase_data(f"Students/student_info/student_index/{index.strip()}")
        if student_info and "student_name" in student_info:
            student_names.append(student_info["student_name"])
            print(f"Found student name: {student_info['student_name']}")
        else:
            print(f"Student info not found for index: {index.strip()}")

    print(f"Student names fetched: {student_names}")
    return student_names

def create_sheet_requests(sheet_title, sheet_metadata, student_names):
    print(f"Creating sheet setup requests for sheet title: {sheet_title}")
    sheet_id = sheet_metadata.get(sheet_title)
    if sheet_id is None:
        print(f"Sheet title '{sheet_title}' not found in metadata.")
        return []

    requests = []

    # Set column widths
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 50},
            "fields": "pixelSize"
        }
    })

    # Add student names to the sheet
    for i, name in enumerate(student_names):
        print(f"Adding student name '{name}' to row {i + 1}, column 0")
        requests.append({
            "updateCells": {
                "rows": [{"values": [{"userEnteredValue": {"stringValue": name}}]}],
                "start": {"sheetId": sheet_id, "rowIndex": i + 1, "columnIndex": 0},
                "fields": "userEnteredValue"
            }
        })
    print("Sheet setup requests created.")
    return requests

def execute_requests(service, spreadsheet_id, requests):
    print("Executing batch update requests...")
    try:
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()
        print("Batch update executed successfully.")
        return response
    except HttpError as e:
        print(f"Error executing batch update: {e}")
        return None

def main():
    print("Starting main process...")
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    course_ids = get_all_course_ids()
    if not course_ids:
        print("No courses to process.")
        return

    for course_id in course_ids:
        print(f"Processing course ID: {course_id}")
        spreadsheet_id = get_sheet_id(course_id)
        if not spreadsheet_id:
            print(f"No spreadsheet ID found for course ID {course_id}. Ending loop.")
            break

        sheet_metadata = get_sheet_metadata(sheets_service, spreadsheet_id)
        if not sheet_metadata:
            print(f"No sheet metadata found for spreadsheet ID {spreadsheet_id}. Ending loop.")
            break

        student_names = get_student_names(course_id)
        if not student_names:
            print(f"No student names found for course ID {course_id}. Ending loop.")
            break

        sheet_title = "Sheet1"  # Replace with actual sheet title if needed
        print(f"Preparing requests for course ID {course_id}...")
        requests = create_sheet_requests(sheet_title, sheet_metadata, student_names)
        if requests:
            execute_requests(sheets_service, spreadsheet_id, requests)
        print(f"Finished processing course ID: {course_id}\n")

if __name__ == "__main__":
    main()
