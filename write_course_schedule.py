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
        print(f"Firebaseデータ取得エラー: {e}")
        return None

def extract_sheet_id(course_id):
    course_data = get_firebase_data(f'Courses/{course_id}/course_sheet_id')
    return course_data if course_data else None

def extract_student_names(course_id):
    student_indices_data = get_firebase_data(f'Students/enrollment/course_id/{course_id}/student_index')
    if not student_indices_data:
        print(f"コースID {course_id} に関連する学生インデックスが見つかりませんでした。")
        return []

    student_indices = student_indices_data.split(', ')
    student_names = []

    for student_index in student_indices:
        student_info = get_firebase_data(f'Students/student_info/student_index/{student_index}')
        if student_info and 'student_name' in student_info:
            student_names.append((student_info['student_name'], student_info.get('attendance_number', "")))

    return student_names

def prepare_update_requests(sheet_id, student_names, month, sheets_service, spreadsheet_id, year=2025):
    if not student_names:
        print("学生名リストが空です。Firebaseから取得したデータを確認してください。")
        return []

    base_title = f"{year}-{str(month).zfill(2)}"
    sheet_title = base_title

    # ユニークなシート名を生成
    existing_sheets = execute_with_retry(
        sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id)
    ).get("sheets", [])
    sheet_titles = [sheet["properties"]["title"] for sheet in existing_sheets]
    counter = 1
    while sheet_title in sheet_titles:
        sheet_title = f"{base_title} ({counter})"
        counter += 1

    add_sheet_request = {"addSheet": {"properties": {"title": sheet_title}}}
    requests = [add_sheet_request]

    # Execute sheet creation
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
        print("新しいシートのIDを取得できませんでした。")
        return []

    # 学生名と出席番号をスプレッドシートに追加
    update_requests = [
        create_cell_update_request(new_sheet_id, 0, 1, "学生名"),
        create_cell_update_request(new_sheet_id, 0, 0, "AN")
    ]

    for i, (name, attendance_number) in enumerate(student_names):
        update_requests.append(create_cell_update_request(new_sheet_id, i + 1, 1, name))
        update_requests.append(create_cell_update_request(new_sheet_id, i + 1, 0, attendance_number))

    return update_requests

def execute_with_retry(request, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return request.execute()
        except (HttpError, socket.timeout) as e:
            print(f"リクエスト失敗 ({attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise

def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": str(value)}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    courses = get_firebase_data('Courses/course_id')
    if not courses or not isinstance(courses, list):
        print("コースデータが見つかりませんでした。")
        return

     for course_id in range(1, len(courses)):
        spreadsheet_id = extract_sheet_id(course_id)
        if not spreadsheet_id:
            print(f\"コースID {course_id} のスプレッドシートIDが見つかりません。\")
            continue
  


        student_names = extract_student_names(course_id)
        if not student_names:
            print(f"コースID {course_id} に関連する学生が見つかりませんでした。")
            continue

        for month in range(1, 13):
            print(f"Processing month: {month} for course ID: {course_id}")
            requests = prepare_update_requests(course_id, student_names, month, sheets_service, spreadsheet_id)
            if not requests:
                print(f"月 {month} のシートを更新するリクエストがありません。")
                continue

            execute_with_retry(
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': requests}
                )
            )
            print(f"月 {month} のシートを正常に更新しました。")

if __name__ == "__main__":
    main()
