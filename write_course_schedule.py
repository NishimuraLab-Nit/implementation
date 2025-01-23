from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
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

# コースIDからシートIDを取得
def get_sheet_id(course_id):
    course_data = get_firebase_data(f"Courses/course_id/{course_id}")
    if course_data and "course_sheet_id" in course_data:
        return course_data["course_sheet_id"]
    else:
        print(f"コース {course_id} に対応するシートIDが見つかりません。")
        return None

# コースIDから学生名を取得
def get_student_names(course_id):
    enrollment_data = get_firebase_data(f"Students/enrollment/{course_id}")
    if not enrollment_data or "student_index" not in enrollment_data:
        print(f"コース {course_id} に対応する学生インデックスが見つかりません。")
        return []

    student_indices = enrollment_data["student_index"].split(",")
    student_names = []

    for student_index in student_indices:
        student_info = get_firebase_data(f"Students/student_info/{student_index.strip()}")
        if student_info and "student_name" in student_info:
            student_names.append(student_info["student_name"])
        else:
            print(f"学生インデックス {student_index} に対応する学生名が見つかりません。")

    return student_names

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

# シート作成リクエスト
def create_sheet_request(sheet_title):
    return {"addSheet": {"properties": {"title": sheet_title}}}

# セル更新リクエスト
def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": str(value)}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

# 土日セルの色付けリクエスト
def create_weekend_color_request(sheet_id, start_row, end_row, start_col, end_col, color):
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col
            },
            "cell": {"userEnteredFormat": {"backgroundColor": color}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    }

# 黒背景リクエスト
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

# シート更新リクエストを準備
def prepare_update_requests(sheet_id, student_names, month, sheets_service, spreadsheet_id, year=2025):
    base_title = f"{year}-{str(month).zfill(2)}"
    add_sheet_request = create_sheet_request(base_title)
    requests = [add_sheet_request]

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

    requests = []

    # 学生名を追加
    requests.append(create_cell_update_request(new_sheet_id, 0, 1, "学生名"))
    for i, student_name in enumerate(student_names):
        requests.append(create_cell_update_request(new_sheet_id, i + 1, 1, student_name))

    # 日付と週末の色設定
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    current_date = start_date
    column_index = 2

    while current_date <= end_date:
        weekday = current_date.weekday()
        if weekday >= 5:  # 土日
            color = {"red": 1.0, "green": 0.9, "blue": 0.9} if weekday == 6 else {"red": 0.9, "green": 1.0, "blue": 0.9}
            requests.append(create_weekend_color_request(new_sheet_id, 0, len(student_names) + 1, column_index, column_index + 1, color))
        requests.append(create_cell_update_request(new_sheet_id, 0, column_index, current_date.strftime("%d")))
        current_date += timedelta(days=1)
        column_index += 1

    return requests

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    courses = get_firebase_data("Courses/course_id")
    if not courses or not isinstance(courses, list):
        print("コースデータが見つかりません。")
        return

    for course_id, course_data in enumerate(courses[1:], start=1):
        if not course_data:
            continue

        sheet_id = get_sheet_id(course_id)
        if not sheet_id:
            continue

        student_names = get_student_names(course_id)
        if not student_names:
            continue

        for month in range(1, 13):
            requests = prepare_update_requests(sheet_id, student_names, month, sheets_service, sheet_id)
            if requests:
                execute_with_retry(
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=sheet_id,
                        body={'requests': requests}
                    )
                )

if __name__ == "__main__":
    main()