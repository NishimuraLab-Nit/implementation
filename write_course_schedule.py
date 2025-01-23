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
        data = db.reference(ref_path).get()
        print(f"DEBUG: {ref_path} -> {data}")
        return data
    except Exception as e:
        print(f"Firebaseデータ取得エラー: {e}")
        return None

# コースIDに対応するsheet_idを取得
def get_sheet_id(course_id):
    course_data = get_firebase_data(f"Courses/course_id/{course_id}")
    if course_data:
        return course_data.get("course_sheet_id", "")
    return ""

# Firebaseから学生名を取得
def get_student_names(course_id):
    student_indices_data = get_firebase_data(f"Students/enrollment/{course_id}/student_index")
    if not student_indices_data:
        print(f"コース {course_id} に対応する学生インデックスが見つかりませんでした。")
        return []

    student_indices = student_indices_data.split(",")
    student_names = []

    for student_index in student_indices:
        student_info = get_firebase_data(f"Students/student_info/{student_index}")
        if student_info and student_info.get("student_name"):
            student_names.append(student_info["student_name"])

    print(f"DEBUG: 学生名一覧 -> {student_names}")
    return student_names

# セル更新リクエストを作成
def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": str(value)}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

# 土日セルの色付けリクエストを作成
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

# 黒背景リクエストを作成
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
def prepare_update_requests(sheet_id, student_names, month, year=2025):
    if not student_names:
        print("学生名リストが空です。Firebaseから取得したデータを確認してください。")
        return []

    requests = []

    # 学生名とヘッダーを記載
    requests.append(create_cell_update_request(sheet_id, 0, 1, "学生名"))
    for i, name in enumerate(student_names):
        requests.append(create_cell_update_request(sheet_id, i + 1, 1, name))

    # 日付と授業時限を設定
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    current_date = start_date
    start_column = 2

    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = f"{current_date.strftime('%m/%d')} ({japanese_weekdays[weekday]})"
        requests.append(create_cell_update_request(sheet_id, 0, start_column, date_string))

        if weekday >= 5:  # 土日カラー設定
            color = {"red": 1.0, "green": 0.8, "blue": 0.8} if weekday == 6 else {"red": 0.8, "green": 0.9, "blue": 1.0}
            requests.append(create_weekend_color_request(sheet_id, 0, len(student_names) + 1, start_column, start_column + 1, color))

        start_column += 1
        current_date += timedelta(days=1)

    # 残りの背景を黒に設定
    requests.append(create_black_background_request(sheet_id, len(student_names) + 1, 1000, 0, 1000))

    return requests

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    courses = get_firebase_data("Courses/course_id")
    if not courses or not isinstance(courses, list):
        print("Coursesデータが見つかりませんでした。")
        return

    for course_id, course_data in enumerate(courses):
        print(f"DEBUG: 処理中のコースID -> {course_id}, データ -> {course_data}")
        if course_id == 0 or not course_data:
            continue

        sheet_id = get_sheet_id(course_id)
        if not sheet_id:
            print(f"コース {course_id} に対応するスプレッドシートIDが見つかりませんでした。")
            continue

        student_names = get_student_names(course_id)
        if not student_names:
            print(f"コース {course_id} に学生名が見つかりませんでした。ループを終了して次に進みます。")
            continue

        print(f"コース {course_id} のスプレッドシートを更新しています...")

        for month in range(1, 13):
            requests = prepare_update_requests(sheet_id, student_names, month)
            if not requests:
                continue

            execute_with_retry(
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={'requests': requests}
                )
            )
            print(f"月 {month} のシートを正常に更新しました。")

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

if __name__ == "__main__":
    main()
