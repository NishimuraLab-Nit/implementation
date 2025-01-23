import time
from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta

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

def create_sheet_request(sheet_title):
    return {
        "addSheet": {
            "properties": {
                "title": sheet_title,
                "gridProperties": {
                    "rowCount": 1000,
                    "columnCount": 32
                }
            }
        }
    }

def execute_request_with_retry(sheets_service, spreadsheet_id, requests, retries=3, delay=5):
    for attempt in range(retries):
        try:
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            ).execute()
            return
        except HttpError as e:
            if e.resp.status == 429 and attempt < retries - 1:
                print(f"Rate limit exceeded. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                raise

def prepare_update_requests(course_sheet_id, student_name, month, sheets_service, spreadsheet_id, year=2025):
    requests = []
    if not student_name:
        print("学生名が空です。")
        return requests

    base_title = f"{year}-{str(month).zfill(2)}"
    add_sheet_request = create_sheet_request(base_title)
    requests.append(add_sheet_request)

    response = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'requests': [add_sheet_request]}
    ).execute()

    new_sheet_id = None
    for reply in response.get('replies', []):
        if 'addSheet' in reply:
            new_sheet_id = reply['addSheet']['properties']['sheetId']
    if new_sheet_id is None:
        print("新しいシートIDを取得できませんでした。")
        return requests

    # 学生名を設定
    requests.append({
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": "学生名"}}]}],
            "start": {"sheetId": new_sheet_id, "rowIndex": 0, "columnIndex": 0},
            "fields": "userEnteredValue"
        }
    })
    requests.append({
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": student_name}}]}],
            "start": {"sheetId": new_sheet_id, "rowIndex": 1, "columnIndex": 0},
            "fields": "userEnteredValue"
        }
    })

    return requests

def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    student_indices = get_firebase_data('Students/student_info/student_index')
    if not student_indices or not isinstance(student_indices, dict):
        print("Firebaseから学生インデックスを取得できませんでした。")
        return

    for student_index, student_data in student_indices.items():
        student_name = student_data.get('student_name')
        if not student_name:
            print(f"学生インデックス {student_index} に学生名が見つかりませんでした。")
            continue

        student_course_ids = get_firebase_data(f'Students/enrollment/student_index/{student_index}/course_id')
        if isinstance(student_course_ids, str):
            student_course_ids = student_course_ids.split(', ')
        if not student_course_ids:
            print(f"学生インデックス {student_index} に関連付けられたコースIDが見つかりませんでした。")
            continue

        for course_id in student_course_ids:
            course_id = course_id.strip()
            if not course_id:
                print(f"無効なコースIDが見つかりました: {course_id}")
                continue

            course_sheet_id = get_firebase_data(f'Courses/course_id/{course_id}/course_sheet_id')
            if not course_sheet_id:
                print(f"コースID {course_id} に関連付けられたシートIDが見つかりませんでした。")
                continue

            for month in range(1, 13):
                print(f"Processing month: {month} for student index: {student_index} and course_id: {course_id}")
                requests = prepare_update_requests(course_sheet_id, student_name, month, sheets_service, course_sheet_id)
                if not requests:
                    print(f"月 {month} のシートを更新するリクエストがありません。")
                    continue

                # リクエストを実行（再試行付き）
                execute_request_with_retry(sheets_service, course_sheet_id, requests)
                print(f"月 {month} のシートを正常に更新しました。")

if __name__ == "__main__":
    main()
