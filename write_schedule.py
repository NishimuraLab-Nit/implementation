import os
from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta

def initialize_firebase():
    """
    Initialize the Firebase app with credentials from the environment variable.
    """
    try:
        firebase_cred_path = os.getenv('FIREBASE_CREDENTIALS_PATH', 'firebase-adminsdk.json')
        firebase_cred = credentials.Certificate(firebase_cred_path)
        initialize_app(firebase_cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })
    except Exception as e:
        print(f"Failed to initialize Firebase: {e}")
        raise

def get_google_sheets_service():
    """
    Initialize and return the Google Sheets API service.
    """
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        google_cred_path = os.getenv('GOOGLE_CREDENTIALS_PATH', 'google-credentials.json')
        google_creds = Credentials.from_service_account_file(google_cred_path, scopes=scopes)
        return build('sheets', 'v4', credentials=google_creds)
    except Exception as e:
        print(f"Failed to initialize Google Sheets service: {e}")
        raise

def get_firebase_data(ref_path):
    """
    Retrieve data from Firebase at the specified reference path.
    """
    try:
        data = db.reference(ref_path).get()
        if data is None:
            print(f"No data found at path: {ref_path}")
        else:
            print(f"Data retrieved from path {ref_path}: {data}")
        return data
    except Exception as e:
        print(f"Failed to get data from Firebase at path {ref_path}: {e}")
        raise

def create_monthly_sheets_request():
    """
    Create requests to add sheets for each month from January to December 2025.
    """
    requests = []
    for month in range(1, 13):
        sheet_title = f"{month}月"
        requests.append({
            "addSheet": {
                "properties": {
                    "title": sheet_title
                }
            }
        })
    return requests

def prepare_monthly_update_requests(sheet_titles, class_names):
    """
    Prepare update requests for each month sheet.
    """
    requests = []
    start_date = datetime(2025, 1, 1)

    for month, sheet_title in enumerate(sheet_titles, start=1):
        sheet_id = month  # Using the month index as sheet_id for simplicity.

        requests.append(create_dimension_request(sheet_id, "ROWS", 0, 1, 120))
        requests.append(create_cell_update_request(sheet_id, 0, 0, "教科"))
        
        # Add class names to the sheet
        for i, class_name in enumerate(class_names):
            requests.append(create_cell_update_request(sheet_id, i + 1, 0, class_name))

        # Add date headers for the current month
        date = start_date + timedelta(days=(month - 1) * 31)
        for day in range(1, 32):
            try:
                current_date = datetime(2025, month, day)
                weekday = current_date.weekday()
                date_string = f"{current_date.day}\n{['月', '火', '水', '木', '金', '土', '日'][weekday]}"
                requests.append(create_cell_update_request(sheet_id, 0, day, date_string))

                if weekday in (5, 6):  # Saturday or Sunday
                    color = {"red": 0.8, "green": 0.9, "blue": 1.0} if weekday == 5 else {"red": 1.0, "green": 0.8, "blue": 0.8}
                    requests.append(create_conditional_formatting_request(
                        sheet_id, 1, len(class_names) + 1, day, day + 1, color,
                        f"=ISNUMBER(SEARCH('{['土', '日'][weekday - 5]}', INDIRECT(ADDRESS(1, COLUMN()))))"
                    ))
            except ValueError:
                break  # Skip invalid dates for months with fewer days

    return requests

def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    student_index = os.getenv('STUDENT_INDEX', 'E534')

    sheet_id_path = f'Students/student_info/student_index/{student_index}/sheet_id'
    course_id_path = f'Students/enrollment/student_index/{student_index}/course_id'
    courses_path = 'Courses/course_id'

    sheet_id = get_firebase_data(sheet_id_path)
    student_course_ids = get_firebase_data(course_id_path)
    courses = get_firebase_data(courses_path)

    print("Sheet ID:", sheet_id)
    print("Student Course IDs:", student_course_ids)
    print("Courses:", courses)

    if not sheet_id or not isinstance(student_course_ids, list) or not isinstance(courses, list):
        print("Invalid data retrieved from Firebase.")
        return

    courses_dict = {str(i): course for i, course in enumerate(courses) if course}
    course_names = [
        courses_dict[cid]['course_name'] for cid in student_course_ids
        if cid in courses_dict and 'course_name' in courses_dict[cid]
    ]

    print("Course Names:", course_names)

    # Create requests for adding sheets and populating them
    sheet_titles = [f"{month}月" for month in range(1, 13)]
    requests = create_monthly_sheets_request()
    requests.extend(prepare_monthly_update_requests(sheet_titles, course_names))

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={'requests': requests}
    ).execute()

    print("Sheets updated successfully.")

if __name__ == "__main__":
    main()
