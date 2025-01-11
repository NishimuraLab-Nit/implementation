from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta

# Constants
FIREBASE_CREDENTIALS_FILE = "firebase-adminsdk.json"
GOOGLE_CREDENTIALS_FILE = "google-credentials.json"
FIREBASE_DATABASE_URL = "https://test-51ebc-default-rtdb.firebaseio.com/"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
MONTHS = [f"{i}月" for i in range(1, 13)]
JAPANESE_WEEKDAYS = ["月", "火", "水", "木", "金", "土", "日"]


def initialize_firebase():
    """Initialize Firebase."""
    try:
        firebase_cred = credentials.Certificate(FIREBASE_CREDENTIALS_FILE)
        initialize_app(firebase_cred, {'databaseURL': FIREBASE_DATABASE_URL})
        print("Firebase initialized successfully.")
    except Exception as e:
        raise RuntimeError(f"Error initializing Firebase: {e}")


def get_google_sheets_service():
    """Get Google Sheets API service."""
    try:
        google_creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=SCOPES)
        return build('sheets', 'v4', credentials=google_creds)
    except Exception as e:
        raise RuntimeError(f"Error initializing Google Sheets API: {e}")


def get_firebase_data(ref_path):
    """Fetch data from Firebase."""
    try:
        data = db.reference(ref_path).get()
        print(f"Data fetched from Firebase ({ref_path}): {data}")
        return data
    except Exception as e:
        print(f"Error fetching data from Firebase ({ref_path}): {e}")
        return None


def validate_firebase_data(sheet_id, student_course_ids, courses):
    """Validate and structure Firebase data."""
    if not isinstance(sheet_id, str) or not sheet_id.strip():
        raise ValueError("Invalid or missing Sheet ID.")

    if isinstance(student_course_ids, int):
        student_course_ids = [str(student_course_ids)]
    elif isinstance(student_course_ids, str):
        student_course_ids = [student_course_ids]
    elif not isinstance(student_course_ids, list) or not student_course_ids:
        raise ValueError("Invalid or missing student course IDs.")

    valid_courses = [course for course in courses if isinstance(course, dict) and 'class_name' in course]
    if not valid_courses:
        raise ValueError("Invalid or incomplete course data.")

    return student_course_ids, valid_courses


def get_existing_sheet_titles(sheets_service, sheet_id):
    """Retrieve existing sheet titles."""
    try:
        response = sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        return [sheet['properties']['title'] for sheet in response.get('sheets', [])]
    except Exception as e:
        print(f"Error retrieving sheet titles: {e}")
        return []


def create_monthly_sheets(sheet_id, sheets_service):
    """Create sheets for each month if they don't already exist."""
    existing_titles = get_existing_sheet_titles(sheets_service, sheet_id)
    requests = [
        {"addSheet": {"properties": {"title": month}}}
        for month in MONTHS if month not in existing_titles
    ]

    if not requests:
        print("All monthly sheets already exist. No new sheets added.")
        return

    try:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id, body={"requests": requests}
        ).execute()
        print("Monthly sheets created successfully.")
    except Exception as e:
        print(f"Error creating monthly sheets: {e}")


def create_cell_update_request(sheet_id, row_index, column_index, value):
    """Create a cell update request for Google Sheets."""
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }


def prepare_update_requests(sheet_id, class_names, month_index):
    """Prepare update requests for Google Sheets."""
    if not class_names:
        print("Class names list is empty. Check Firebase data.")
        return []

    requests = [create_cell_update_request(sheet_id, 0, 0, "教科")]
    requests.extend(
        create_cell_update_request(sheet_id, i + 1, 0, name) for i, name in enumerate(class_names)
    )

    start_date = datetime(2025, month_index + 1, 1)
    for i in range(31):
        date = start_date + timedelta(days=i)
        if date.month != month_index + 1:
            break
        weekday = JAPANESE_WEEKDAYS[date.weekday()]
        date_string = f"{date.strftime('%m/%d')} ({weekday})"
        requests.append(create_cell_update_request(sheet_id, 0, i + 1, date_string))

    return requests


def main():
    """Main function."""
    try:
        initialize_firebase()
        sheets_service = get_google_sheets_service()

        sheet_id = get_firebase_data('Students/student_info/student_index/E534/sheet_id')
        student_course_ids = get_firebase_data('Students/enrollment/student_index/E534/course_id')
        courses = get_firebase_data('Courses/course_id')

        student_course_ids, courses = validate_firebase_data(sheet_id, student_course_ids, courses)
        courses_dict = {str(i): course for i, course in enumerate(courses)}

        class_names = [
            courses_dict[cid]['class_name'] for cid in student_course_ids
            if cid in courses_dict and 'class_name' in courses_dict[cid]
        ]

        create_monthly_sheets(sheet_id, sheets_service)

        for month_index in range(12):
            requests = prepare_update_requests(sheet_id, class_names, month_index)
            if not requests:
                print(f"No update requests for {month_index + 1}月.")
                continue

            try:
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id, body={'requests': requests}
                ).execute()
                print(f"{month_index + 1}月 sheet updated successfully.")
            except Exception as e:
                print(f"Error updating {month_index + 1}月 sheet: {e}")

    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
