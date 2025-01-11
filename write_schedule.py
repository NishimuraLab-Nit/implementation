import os
from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime

def initialize_firebase():
    firebase_cred = credentials.Certificate("firebase-adminsdk.json")
    initialize_app(firebase_cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })


def get_google_sheets_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    return build('sheets', 'v4', credentials=google_creds)


def get_firebase_data(ref_path):
    return db.reference(ref_path).get()


def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }


def create_dimension_request(sheet_id, dimension, start_index, end_index, pixel_size):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": dimension, "startIndex": start_index, "endIndex": end_index},
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize"
        }
    }


def create_conditional_formatting_request(sheet_id, start_row, end_row, start_col, end_col, color, formula):
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
    black_color = {"red": 0.0, "green": 0.0, "blue": 0.0}
    return {
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                      "startColumnIndex": start_col, "endColumnIndex": end_col},
            "cell": {"userEnteredFormat": {"backgroundColor": black_color}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    }


def prepare_update_requests(sheet_id, class_names):
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
    for month, sheet_title in enumerate(sheet_ids.keys(), start=1):
        sheet_id = sheet_ids[sheet_title]

        # Set row height and add column headers
        requests.append(create_dimension_update(sheet_id, "ROWS", 0, 1, 120))
        requests.append(create_cell_update(sheet_id, 0, 0, "教科"))

        # Add class names in the first column
        for i, class_name in enumerate(class_names):
            requests.append(create_cell_update(sheet_id, i + 1, 0, class_name))

        # Ensure the sheet has at least 32 columns
        requests.append({
            "appendDimension": {
                "sheetId": sheet_id,
                "dimension": "COLUMNS",
                "length": 32  # Extend to 32 columns (for 31 days + 1 column for class names)
            }
        })

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
