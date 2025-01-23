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

# 列幅と行高さを設定するリクエスト
def set_sheet_dimensions(sheet_id, new_sheet_id, num_rows, num_columns):
    requests = []

    # 列幅を設定 (学生名、出席番号、日付列)
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": new_sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 50},
            "fields": "pixelSize"
        }
    })
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": new_sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},
            "properties": {"pixelSize": 150},
            "fields": "pixelSize"
        }
    })
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": new_sheet_id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": num_columns},
            "properties": {"pixelSize": 80},
            "fields": "pixelSize"
        }
    })

    # 行高さを設定
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": new_sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 40},
            "fields": "pixelSize"
        }
    })
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": new_sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": num_rows},
            "properties": {"pixelSize": 30},
            "fields": "pixelSize"
        }
    })

    return requests

# シート全体の背景色を設定するリクエスト
def set_sheet_background(sheet_id, new_sheet_id, num_rows, num_columns):
    requests = []

    # 背景色を白に設定 (全体)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": new_sheet_id,
                "startRowIndex": 0,
                "endRowIndex": num_rows,
                "startColumnIndex": 0,
                "endColumnIndex": num_columns
            },
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": 1, "green": 1, "blue": 1}}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    })

    # ヘッダー行の背景色を設定
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": new_sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 1,
                "startColumnIndex": 0,
                "endColumnIndex": num_columns
            },
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    })

    return requests

# 日付を追加
def add_dates_to_sheet(sheet_id, month, year, new_sheet_id):
    requests = []
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    total_days = (end_date - start_date).days + 1
    
    column_index = 2
    current_date = start_date

    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = f"{current_date.strftime('%m/%d')} ({japanese_weekdays[weekday]})"

        requests.append({
            "updateCells": {
                "rows": [{"values": [{"userEnteredValue": {"stringValue": date_string}}]}],
                "start": {"sheetId": new_sheet_id, "rowIndex": 0, "columnIndex": column_index},
                "fields": "userEnteredValue"
            }
        })

        column_index += 1
        current_date += timedelta(days=1)

    return requests

# シート更新リクエストを準備
def prepare_update_requests(sheet_id, student_names, attendance_numbers, month, sheets_service, spreadsheet_id, year=2025):
    if not student_names:
        print("学生名リストが空です。")
        return []

    base_title = f"{year}-{str(month).zfill(2)}"
    response = execute_with_retry(
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': [{"addSheet": {"properties": {"title": base_title}}}]}
        )
    )
    new_sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']

    requests = []
    num_columns = 2 + 31
    num_rows = len(student_names) + 1

    requests.extend(set_sheet_dimensions(sheet_id, new_sheet_id, num_rows, num_columns))
    requests.extend(set_sheet_background(sheet_id, new_sheet_id, num_rows, num_columns))
    requests.extend(add_dates_to_sheet(sheet_id, month, year, new_sheet_id))

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
        sheet_id = get_sheet_id(course_id)
        if not sheet_id:
            continue

        student_names, attendance_numbers = get_students_by_course(course_id)
        if not student_names:
            continue

        for month in range(1, 13):
            requests = prepare_update_requests(sheet_id, student_names, attendance_numbers, month, sheets_service, sheet_id)
            if requests:
                execute_with_retry(
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=sheet_id,
                        body={'requests': requests}
                    )
                )

if __name__ == "__main__":
    main()
