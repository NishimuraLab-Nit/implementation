from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta

def initialize_firebase():
    """
    Initialize Firebase app using credentials.
    """
    try:
        firebase_cred = credentials.Certificate("firebase-adminsdk.json")
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
        google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
        return build('sheets', 'v4', credentials=google_creds)
    except Exception as e:
        print(f"Failed to initialize Google Sheets service: {e}")
        raise

def get_firebase_data(ref_path):
    """
    Retrieve data from Firebase.
    """
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Failed to get data from Firebase at {ref_path}: {e}")
        raise

def create_cell_update_request(sheet_id, row_index, column_index, value):
    """
    Create a request to update a specific cell.
    """
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

def create_dimension_request(sheet_id, dimension, start_index, end_index, pixel_size):
    """
    Create a request to update dimensions (row or column size).
    """
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": dimension, "startIndex": start_index, "endIndex": end_index},
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize"
        }
    }

def create_conditional_formatting_request(sheet_id, start_row, end_row, start_col, end_col, color, formula):
    """
    Create a conditional formatting request for a specific range.
    """
    return {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                            "startColumnIndex": start_col, "endColumnIndex": end_col}],
                "booleanRule": {
                    "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": formula}]},
                    "format": {"backgroundColor": color}
                }
            },
            "index": 0
        }
    }

def create_black_background_request(sheet_id, start_row, end_row, start_col, end_col):
    """
    Create a request to fill a range with a black background.
    """
    return {
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                      "startColumnIndex": start_col, "endColumnIndex": end_col},
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    }

def prepare_update_requests(sheet_id, class_names):
    """
    Prepare the list of requests to update the Google Sheet.
    """
    if not class_names:
        print("No class names provided.")
        return []

    requests = []
    requests.append(create_dimension_request(sheet_id, "ROWS", 0, 1, 120))  # Header row height
    requests.append(create_dimension_request(sheet_id, "COLUMNS", 0, 32, 100))  # Column widths

    # Add header
    requests.append(create_cell_update_request(sheet_id, 0, 0, "教科"))

    # Add class names
    for i, class_name in enumerate(class_names):
        requests.append(create_cell_update_request(sheet_id, i + 1, 0, class_name))

    # Add dates and conditional formatting
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(2023, 12, 1)

    for i in range(31):
        current_date = start_date + timedelta(days=i)
        if current_date.month != 12:
            break

        weekday = current_date.weekday()
        date_string = (
            f"{current_date.strftime('%m')}\\n月\\n"
            f"{current_date.strftime('%d')}\\n日\\n⌢\\n"
            f"{japanese_weekdays[weekday]}\\n⌣"
        )
        requests.append(create_cell_update_request(sheet_id, 0, i + 1, date_string))

        if weekday in (5, 6):  # Saturday or Sunday
            color = {"red": 0.8, "green": 0.9, "blue": 1.0} if weekday == 5 else {"red": 1.0, "green": 0.8, "blue": 0.8}
            formula = f"=ISNUMBER(SEARCH(\"{japanese_weekdays[weekday]}\", INDIRECT(ADDRESS(1, COLUMN()))))"
            requests.append(create_conditional_formatting_request(sheet_id, 1, len(class_names) + 1, i + 1, i + 2, color, formula))

    return requests

def main():
    """
    Main function to update the Google Sheet with data and formatting.
    """
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # Firebase paths
    sheet_id_path = 'Students/item/student_number/e19139/sheet_id'
    course_id_path = 'Students/enrollment/student_number/e19139/course_id'
    courses_path = 'Courses/course_id'

    # Retrieve data
    sheet_id = get_firebase_data(sheet_id_path)
    student_course_ids = get_firebase_data(course_id_path)
    courses = get_firebase_data(courses_path)

    if not sheet_id or not isinstance(student_course_ids, list) or not isinstance(courses, list):
        print("Invalid data retrieved from Firebase.")
        return

    # Map course IDs to class names
    courses_dict = {i: course for i, course in enumerate(courses) if course}
    class_names = [
        courses_dict[cid]['class_name'] for cid in student_course_ids
        if cid in courses_dict and 'class_name' in courses_dict[cid]
    ]

    # Prepare update requests
    requests = prepare_update_requests(sheet_id, class_names)
    if not requests:
        print("No update requests prepared.")
        return

    # Execute update
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": requests}
    ).execute()
    print("Sheet updated successfully.")

if __name__ == "__main__":
    main()
