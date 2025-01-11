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



def create_cell_update_request(sheet_id, row_index, column_index, value):
    """
    Create a request to update a specific cell in the Google Sheet.
    """
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }


def prepare_update_requests(sheet_id, class_names):
    """
    Prepare a batch of requests to update the Google Sheet with class names and date headers.
    """
    if not class_names:
        print("Class names list is empty. Check data retrieved from Firebase.")
        return []

    requests = [
        {"appendDimension": {"sheetId": 0, "dimension": "COLUMNS", "length": 32}},
        create_dimension_request(0, "COLUMNS", 0, 1, 100),
        create_dimension_request(0, "COLUMNS", 1, 32, 35),
        create_dimension_request(0, "ROWS", 0, 1, 120),
        {"repeatCell": {"range": {"sheetId": 0},
                        "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                        "fields": "userEnteredFormat.horizontalAlignment"}},
        {"updateBorders": {"range": {"sheetId": 0, "startRowIndex": 0, "endRowIndex": 25, "startColumnIndex": 0,
                                     "endColumnIndex": 32},
                           "top": {"style": "SOLID", "width": 1},
                           "bottom": {"style": "SOLID", "width": 1},
                           "left": {"style": "SOLID", "width": 1},
                           "right": {"style": "SOLID", "width": 1}}},
        {"setBasicFilter": {"filter": {"range": {"sheetId": 0, "startRowIndex": 0, "endRowIndex": 25,
                                                 "startColumnIndex": 0, "endColumnIndex": 32}}}}
    ]

    requests.append(create_cell_update_request(0, 0, 0, "教科"))
    requests.extend(create_cell_update_request(0, i + 1, 0, name) for i, name in enumerate(class_names))

    # Handle dates
    add_date_headers_and_formatting(requests)

    requests.append(create_black_background_request(0, 25, 1000, 0, 1000))
    requests.append(create_black_background_request(0, 0, 1000, 32, 1000))

    return requests


def add_date_headers_and_formatting(requests):
    """
    Add date headers and conditional formatting for weekends to the request list.
    """
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(2025, 1, 1)
    end_row = 25

    for i in range(31):
        date = start_date + timedelta(days=i)
        if date.month != 11:
            break
        weekday = date.weekday()
        date_string = f"{date.strftime('%m')}\n月\n{date.strftime('%d')}\n日\n⌢\n{japanese_weekdays[weekday]}\n⌣"
        requests.append(create_cell_update_request(0, 0, i + 1, date_string))

        if weekday in (5, 6):
            color = {"red": 0.8, "green": 0.9, "blue": 1.0} if weekday == 5 else {"red": 1.0, "green": 0.8, "blue": 0.8}
            requests.append(create_conditional_formatting_request(
                0, 0, end_row, i + 1, i + 2, color,
                f'=ISNUMBER(SEARCH("{japanese_weekdays[weekday]}", INDIRECT(ADDRESS(1, COLUMN()))))'
            ))


def main():
    try:
        initialize_firebase()
        sheets_service = get_google_sheets_service()

        sheet_id = get_firebase_data('Students/student_info/student_index/{student_index}/sheet_id')
        student_course_ids = get_firebase_data('Students/enrollment/student_index/{student_index}/course_id')
        courses = get_firebase_data('Courses/course_id')

        print("Sheet ID:", sheet_id)
        print("Student Course IDs:", student_course_ids)
        print("Courses:", courses)

        if not sheet_id or not isinstance(student_course_ids, list) or not isinstance(courses, list):
            print("Invalid data retrieved from Firebase.")
            return

        courses_dict = {i: course for i, course in enumerate(courses) if course}
        class_names = [
            courses_dict[cid]['class_name'] for cid in student_course_ids
            if cid in courses_dict and 'class_name' in courses_dict[cid]
        ]

        requests = prepare_update_requests(sheet_id, class_names)
        if not requests:
            print("No requests to update the sheet.")
            return

        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={'requests': requests}
        ).execute()
        print("Sheet updated successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
