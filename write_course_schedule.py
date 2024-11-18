from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta

def initialize_firebase():
    # Initialize Firebase
    firebase_cred = credentials.Certificate("firebase-adminsdk.json")
    initialize_app(firebase_cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

def get_google_sheets_service():
    # Get Google Sheets API service
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    return build('sheets', 'v4', credentials=google_creds)

def get_firebase_data(ref_path):
    # Get data from Firebase
    return db.reference(ref_path).get()

def create_cell_update_request(sheet_id, row_index, column_index, value):
    # Create cell update request
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

def prepare_update_requests(sheet_id, class_names):
    if not class_names:
        print("Class names list is empty. Check data retrieved from Firebase.")
        return []

    requests = [
        {"appendDimension": {"sheetId": 0, "dimension": "COLUMNS", "length": 32}},
        {"repeatCell": {"range": {"sheetId": 0},
                        "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                        "fields": "userEnteredFormat.horizontalAlignment"}}
    ]

    requests.append(create_cell_update_request(0, 0, 0, "教科"))
    requests.extend(create_cell_update_request(0, i + 1, 0, name) for i, name in enumerate(class_names))

    return requests

def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    courses = get_firebase_data('Courses/course_id')
    student_info = get_firebase_data('Students/student_info/student_number')

    if not isinstance(courses, list) or not isinstance(student_info, dict):
        print("Invalid data retrieved from Firebase.")
        return

    # Iterate over each course and update the corresponding sheet
    for course in courses:
        if course and 'course_sheet_id' in course:
            sheet_id = course['course_sheet_id']

            # Retrieve the student numbers for this course
            course_id = courses.index(course)
            enrollment_path = f'Students/enrollment/student_number'
            enrolled_students = get_firebase_data(enrollment_path)

            if not enrolled_students:
                print(f"No enrolled students found for course at index {course_id}.")
                continue

            # Collect student names based on the retrieved student numbers
            student_names = [
                student_info[student_number]['student_name']
                for student_number in enrolled_students
                if student_number in student_info and 'student_name' in student_info[student_number]
            ]

            requests = prepare_update_requests(sheet_id, student_names)
            if not requests:
                print(f"No requests to update the sheet {sheet_id}.")
                continue

            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={'requests': requests}
            ).execute()
            print(f"Sheet {sheet_id} updated successfully.")
        else:
            print("Course data is incomplete or missing 'course_sheet_id'.")

if __name__ == "__main__":
    main()
