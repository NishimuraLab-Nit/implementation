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
    firebase_cred = credentials.Certificate("firebase-adminsdk.json")
    initialize_app(firebase_cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Google Sheets APIサービスの初期化
def get_google_sheets_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    authorized_http = AuthorizedHttp(google_creds, http=httplib2.Http(timeout=60))
    return build('sheets', 'v4', cache_discovery=False, http=authorized_http)

# Firebaseからデータを取得
def get_firebase_data(ref_path):
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Firebaseデータ取得エラー: {e}")
        return None

# リトライ付きリクエスト実行
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

# course_sheet_id を取得
def get_sheet_id(course_id):
    course_data = get_firebase_data(f"Courses/course_id/{course_id}")
    if course_data and "course_sheet_id" in course_data:
        return course_data["course_sheet_id"]
    print(f"Course ID {course_id} の course_sheet_id が見つかりません。")
    return None

# 学生データを取得
def get_students_by_course(course_id):
    enrollment_data = get_firebase_data(f"Students/enrollment/course_id/{course_id}")
    if not enrollment_data or "student_index" not in enrollment_data:
        print(f"Course ID {course_id} の学生データが見つかりません。")
        return [], []

    student_indices = enrollment_data["student_index"].split(",")
    student_names = []
    attendance_numbers = []

    for student_index in student_indices:
        student_info = get_firebase_data(f"Students/student_info/student_index/{student_index.strip()}")
        if student_info:
            student_name = student_info.get("student_name")
            attendance_number = student_info.get("attendance_number")
            if student_name:
                student_names.append(student_name)
                attendance_numbers.append(attendance_number or "")
            else:
                print(f"学生インデックス {student_index} の名前が見つかりません。")
        else:
            print(f"学生インデックス {student_index} の情報が見つかりません。")

    return student_names, attendance_numbers

# ユニークなシート名を生成
def generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title):
    existing_sheets = execute_with_retry(
        sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id)
    ).get("sheets", [])
    sheet_titles = [sheet["properties"]["title"] for sheet in existing_sheets]
    title = base_title
    counter = 1
    while title in sheet_titles:
        title = f"{base_title} ({counter})"
        counter += 1
    return title

# シート更新リクエストを準備
def prepare_update_requests(sheet_id, student_names, attendance_numbers, month, sheets_service, spreadsheet_id, year=2025):
    if not student_names:
        print("学生名リストが空です。")
        return []

    # ユニークなシート名を生成
    base_title = f"{year}-{str(month).zfill(2)}"
    sheet_title = generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title)

    # シートを追加するリクエスト
    add_sheet_request = {
        "addSheet": {"properties": {"title": sheet_title}}
    }
    requests = [add_sheet_request]

    # 新しいシートの作成
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
    if new_sheet_id is None:
        print("新しいシートのIDを取得できませんでした。")
        return []

    # 列幅、行高の設定
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": new_sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 126},
            "properties": {"pixelSize": 120},
            "fields": "pixelSize"
        }
    })
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": new_sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1000},
            "properties": {"pixelSize": 30},
            "fields": "pixelSize"
        }
    })

    # 学生名と出席番号を記載
    requests.append({
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": "学生名"}}]}],
            "start": {"sheetId": new_sheet_id, "rowIndex": 0, "columnIndex": 1},
            "fields": "userEnteredValue"
        }
    })
    requests.append({
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": "AN"}}]}],
            "start": {"sheetId": new_sheet_id, "rowIndex": 0, "columnIndex": 0},
            "fields": "userEnteredValue"
        }
    })

    for i, (name, number) in enumerate(zip(student_names, attendance_numbers)):
        requests.append({
            "updateCells": {
                "rows": [{"values": [{"userEnteredValue": {"stringValue": name}}]}],
                "start": {"sheetId": new_sheet_id, "rowIndex": i + 1, "columnIndex": 1},
                "fields": "userEnteredValue"
            }
        })
        requests.append({
            "updateCells": {
                "rows": [{"values": [{"userEnteredValue": {"stringValue": str(number)}}]}],
                "start": {"sheetId": new_sheet_id, "rowIndex": i + 1, "columnIndex": 0},
                "fields": "userEnteredValue"
            }
        })

    # シート全体のデフォルト背景色を白に設定
    requests.append({
        "repeatCell": {
            "range": {"sheetId": new_sheet_id},
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": 1, "green": 1, "blue": 1}}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    })

    return requests

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    courses = get_firebase_data("Courses/course_id")
    if not courses or not isinstance(courses, list):
        print("Courses データが見つかりません。")
        return

    for course_id in range(1, len(courses)):
        print(f"Processing Course ID: {course_id}")
        sheet_id = get_sheet_id(course_id)
        if not sheet_id:
            continue

        student_names, attendance_numbers = get_students_by_course(course_id)
        if not student_names:
            print(f"Course ID {course_id} の学生リストが空です。")
            continue

        for month in range(1, 13):
            print(f"Processing month: {month} for Course ID: {course_id}")
            requests = prepare_update_requests(sheet_id, student_names, attendance_numbers, month, sheets_service, sheet_id)
            if not requests:
                print(f"月 {month} のシートを更新するリクエストがありません。")
                continue

            execute_with_retry(
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={'requests': requests}
                )
            )
            print(f"月 {month} のシートを正常に更新しました。")

if __name__ == "__main__":
    main()
