import os
from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime

def initialize_firebase():
    """
    Initialize Firebase app using credentials from an environment variable.
    """
    try:
        firebase_cred_path = os.getenv('FIREBASE_CREDENTIALS_PATH', 'firebase-adminsdk.json')
        firebase_cred = credentials.Certificate(firebase_cred_path)
        initialize_app(firebase_cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
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
        print(f"Error initializing Google Sheets service: {e}")
        raise

def get_firebase_data(reference_path):
    """
    Retrieve data from Firebase at the specified reference path.
    """
    try:
        data = db.reference(reference_path).get()
        if data is None:
            print(f"No data found at path: {reference_path}")
        return data
    except Exception as e:
        print(f"Error retrieving data from Firebase at path {reference_path}: {e}")
        raise

def create_dimension_update(sheet_id, dimension, start_index, end_index, pixel_size):
    """
    Create a request to update dimensions (rows/columns) in a Google Sheet.
    """
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": dimension, "startIndex": start_index, "endIndex": end_index},
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize"
        }
    }

def create_cell_update(sheet_id, row_index, column_index, value):
    """
    Create a request to update a specific cell in a Google Sheet.
    """
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

def create_conditional_format(sheet_id, start_row, end_row, start_col, end_col, color, formula):
    """
    Create a request to apply conditional formatting to a range in a Google Sheet.
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

def create_monthly_sheets(sheets_service, spreadsheet_id):
    """
    Create sheets for each month if they do not already exist, and return their IDs.
    """
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing_sheets = {sheet['properties']['title']: sheet['properties']['sheetId'] 
                       for sheet in spreadsheet.get('sheets', [])}
    
    sheet_ids = {}

    for month in range(1, 13):
        sheet_title = f"{month}月"
        if sheet_title in existing_sheets:
            print(f"Sheet '{sheet_title}' already exists.")
            sheet_ids[sheet_title] = existing_sheets[sheet_title]
        else:
            requests = [{"addSheet": {"properties": {"title": sheet_title}}}]
            response = sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests}
            ).execute()
            sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
            sheet_ids[sheet_title] = sheet_id
            print(f"Sheet '{sheet_title}' created with ID: {sheet_id}")

    return sheet_ids

def prepare_update_requests(sheet_ids, class_names):
    """
    Prepare requests to format and update each monthly sheet with headers and class names.
    """
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    requests = []

    for month, sheet_title in enumerate(sheet_ids.keys(), start=1):
        sheet_id = sheet_ids[sheet_title]

        # Set row height and add column headers
        requests.append(create_dimension_update(sheet_id, "ROWS", 0, 1, 120))
        requests.append(create_cell_update(sheet_id, 0, 0, "教科"))

        # Add class names in the first column
        for i, class_name in enumerate(class_names):
            requests.append(create_cell_update(sheet_id, i + 1, 0, class_name))

        # Add date headers with weekday labels
        for day in range(1, 32):  # Limit to 31 days
            try:
                current_date = datetime(2025, month, day)
                weekday = current_date.weekday()  # 0 = Monday, 6 = Sunday
                date_string = (
                    f"{current_date.strftime('%m')}月\n"
                    f"{current_date.strftime('%d')}日\n"
                    f"{weekdays[weekday]}"
                )
                requests.append(create_cell_update(sheet_id, 0, day, date_string))

                # Highlight weekends
                if weekday in (5, 6):  # Saturday or Sunday
                    color = {"red": 0.8, "green": 0.9, "blue": 1.0} if weekday == 5 else {"red": 1.0, "green": 0.8, "blue": 0.8}
                    formula = f"=TEXT(INDIRECT(ADDRESS(1, COLUMN())), \"\")=\"{weekdays[weekday]}\""
                    requests.append(create_conditional_format(sheet_id, 1, len(class_names) + 1, day, day + 1, color, formula))
            except ValueError:
                break  # Skip invalid dates beyond the month's end

    return requests

def main():
    """
    Main function to initialize services, retrieve data, and update Google Sheets.
    """
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    student_index = os.getenv('STUDENT_INDEX', 'E534')

    sheet_id_path = f'Students/student_info/student_index/{student_index}/sheet_id'
    course_id_path = f'Students/enrollment/student_index/{student_index}/course_id'
    courses_path = 'Courses/course_id'

    sheet_id = get_firebase_data(sheet_id_path)
    student_course_ids = get_firebase_data(course_id_path)
    courses = get_firebase_data(courses_path)

    if not sheet_id or not isinstance(student_course_ids, list) or not isinstance(courses, list):
        print("Invalid data retrieved from Firebase.")
        return

    courses_dict = {str(i): course for i, course in enumerate(courses) if course}
    course_names = [
        courses_dict[cid]['course_name'] for cid in student_course_ids
        if cid in courses_dict and 'course_name' in courses_dict[cid]
    ]

    # Create monthly sheets and prepare update requests
    sheet_ids = create_monthly_sheets(sheets_service, sheet_id)
    requests = prepare_update_requests(sheet_ids, course_names)

    # Execute batch update
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={'requests': requests}
    ).execute()

    print("Sheets updated successfully.")

if __name__ == "__main__":
    main()
