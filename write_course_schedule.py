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

# Firebaseの初期化
def initialize_firebase():
    try:
        firebase_cred = credentials.Certificate("firebase-adminsdk.json")
        initialize_app(firebase_cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })
        print("Firebase initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize Firebase: {e}")

# Google Sheets APIサービスの初期化
def get_google_sheets_service():
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
        authorized_http = AuthorizedHttp(google_creds, http=httplib2.Http(timeout=60))
        print("Google Sheets API service initialized successfully.")
        return build('sheets', 'v4', cache_discovery=False, http=authorized_http)
    except Exception as e:
        print(f"Failed to initialize Google Sheets API service: {e}")
        return None

# Firebaseからデータを取得
def get_firebase_data(ref_path):
    try:
        data = db.reference(ref_path).get()
        print(f"Data retrieved from {ref_path}: {data}")
        return data
    except Exception as e:
        print(f"Error retrieving data from {ref_path}: {e}")
        return None

# コースごとの学生データを取得
def get_student_data(course_id):
    try:
        enrollment_data = get_firebase_data(f'Students/enrollment/course_id/{course_id}/student_index')
        if not enrollment_data:
            print(f"No enrollment data found for course ID {course_id}. Skipping.")
            return []

        student_indices = [index.strip() for index in enrollment_data.split(',')]
        student_names = []

        for student_index in student_indices:
            student_info = get_firebase_data(f'Students/student_info/{student_index}')
            if student_info and 'student_name' in student_info:
                student_names.append(student_info['student_name'])

        print(f"Student names for course ID {course_id}: {student_names}")
        return student_names
    except Exception as e:
        print(f"Error retrieving student data for course ID {course_id}: {e}")
        return []

# コースのシートIDを取得
def get_course_sheet_id(course_id):
    try:
        course_data = get_firebase_data(f'Courses/{course_id}')
        if course_data and 'course_sheet_id' in course_data:
            sheet_id = course_data['course_sheet_id']
            print(f"Sheet ID for course ID {course_id}: {sheet_id}")
            return sheet_id
        print(f"No sheet ID found for course ID {course_id}. Skipping.")
        return None
    except Exception as e:
        print(f"Error retrieving sheet ID for course ID {course_id}: {e}")
        return None

# リトライ付きリクエスト実行
def execute_with_retry(request, retries=3, delay=5):
    for attempt in range(retries):
        try:
            result = request.execute()
            print("Request executed successfully.")
            return result
        except (HttpError, socket.timeout) as e:
            print(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                print("All retries failed.")
                raise

# セル更新リクエストを作成
def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": str(value)}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

# シート作成リクエスト
def create_sheet_request(sheet_title):
    return {
        "addSheet": {
            "properties": {"title": sheet_title}
        }
    }

# シート次元設定リクエスト
def create_dimension_request(sheet_id, dimension, start_index, end_index, pixel_size):
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": dimension,
                "startIndex": start_index,
                "endIndex": end_index
            },
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize"
        }
    }

# 日付とセルフォーマットを追加するリクエストを準備
def prepare_update_requests(course_id, student_names, month, sheets_service, spreadsheet_id, year=2025):
    if not student_names:
        print(f"No student names provided for course ID {course_id}. Skipping.")
        return []

    base_title = f"{year}-{str(month).zfill(2)}"
    print(f"Preparing requests for sheet titled: {base_title}")

    requests = [create_sheet_request(base_title)]
    response = execute_with_retry(
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        )
    )

    new_sheet_id = next(
        (reply['addSheet']['properties']['sheetId'] for reply in response.get('replies', []) if 'addSheet' in reply),
        None
    )
    if not new_sheet_id:
        print(f"Failed to create or retrieve new sheet ID for course ID {course_id}. Skipping.")
        return []

    print(f"New sheet ID for course ID {course_id}: {new_sheet_id}")

    # Prepare additional formatting requests
    requests.append(create_cell_update_request(new_sheet_id, 0, 1, "Student Name"))
    for i, name in enumerate(student_names):
        requests.append(create_cell_update_request(new_sheet_id, i + 1, 1, name))

    return requests

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()
    if not sheets_service:
        print("Google Sheets service not initialized. Exiting.")
        return

    courses = get_firebase_data('Courses')
    if not courses or not isinstance(courses, dict):
        print("No courses found in Firebase.")
        return

    for course_id, course_data in courses.items():
        if not course_id.isdigit():
            print(f"Invalid course ID format: {course_id}. Skipping.")
            continue

        course_id = int(course_id)
        spreadsheet_id = get_course_sheet_id(course_id)
        if not spreadsheet_id:
            continue

        student_names = get_student_data(course_id)
        if not student_names:
            print(f"No students found for course ID {course_id}. Skipping.")
            continue

        for month in range(1, 13):
            print(f"Processing month {month} for course ID {course_id}.")
            requests = prepare_update_requests(course_id, student_names, month, sheets_service, spreadsheet_id)
            if not requests:
                print(f"No requests prepared for month {month} for course ID {course_id}. Skipping.")
                continue

            try:
                execute_with_retry(
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={'requests': requests}
                    )
                )
                print(f"Sheet for month {month} updated successfully for course ID {course_id}.")
            except Exception as e:
                print(f"Error updating sheet for month {month} for course ID {course_id}: {e}")

if __name__ == "__main__":
    main()
