import time
from datetime import datetime, timedelta
from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

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

def create_black_background_request(sheet_id, start_row, end_row, start_col, end_col):
    black_color = {"red": 0.0, "green": 0.0, "blue": 0.0}
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col
            },
            "cell": {"userEnteredFormat": {"backgroundColor": black_color}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    }

def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
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
                delay *= 2
            else:
                raise

def prepare_update_requests(course_sheet_id, attendance_number, student_name, month, sheets_service, spreadsheet_id, year=2025):
    requests = []
    if not student_name:
        print("学生名が空です。")
        return requests

    base_title = f"{year}-{str(month).zfill(2)}"
    add_sheet_request = create_sheet_request(base_title)
    print(f"Adding sheet with title: {base_title}")  # デバッグ出力
    requests.append(add_sheet_request)

    response = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'requests': [add_sheet_request]}
    ).execute()

    print(f"Google Sheets API response: {response}")  # デバッグ出力

    new_sheet_id = None
    for reply in response.get('replies', []):
        if 'addSheet' in reply:
            new_sheet_id = reply['addSheet']['properties']['sheetId']
    print(f"新しいシートID: {new_sheet_id}")  # デバッグ出力
    if new_sheet_id is None:
        print("新しいシートIDを取得できませんでした。")
        return requests

    # 列幅と行の高さの設定
    requests += [
        create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 50),  # attendance_numberの列幅
        create_dimension_request(new_sheet_id, "COLUMNS", 1, 2, 150),  # student_nameの列幅
        create_dimension_request(new_sheet_id, "COLUMNS", 2, 32, 100),  # 日付の列幅
        create_dimension_request(new_sheet_id, "ROWS", 0, 1, 40)  # ヘッダー行の高さ
    ]

    # ヘッダー設定
    requests += [
        create_cell_update_request(new_sheet_id, 0, 0, "出席番号"),
        create_cell_update_request(new_sheet_id, 0, 1, "学生名"),
        create_cell_update_request(new_sheet_id, 1, 0, str(attendance_number)),
        create_cell_update_request(new_sheet_id, 1, 1, student_name)
    ]

    # 日付と色設定
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    current_date = start_date
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    end_row = 25

    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = f"{current_date.strftime('%m')}\n{current_date.strftime('%d')}\n{japanese_weekdays[weekday]}"
        column_index = 2 + (current_date.day - 1)

        # 日付入力
        requests.append(create_cell_update_request(new_sheet_id, 0, column_index, date_string))

        # 土曜・日曜のセルの色設定
        if weekday in (5, 6):
            color = {"red": 0.8, "green": 0.9, "blue": 1.0} if weekday == 5 else {"red": 1.0, "green": 0.8, "blue": 0.8}
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": new_sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": end_row,
                        "startColumnIndex": column_index,
                        "endColumnIndex": column_index + 1
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": color}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
        current_date += timedelta(days=1)

    # 黒背景の設定
    requests.append(create_black_background_request(new_sheet_id, 25, 1000, 0, 1000))
    requests.append(create_black_background_request(new_sheet_id, 0, 1000, 32, 1000))

    return requests

def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    student_indices = get_firebase_data('Students/enrollment/student_index')
    print("取得したデータ:", student_indices)  

    if not student_indices or not isinstance(student_indices, dict):
        print("Firebaseから学生インデックスを取得できませんでした。")
        return

    for student_index, student_data in student_indices.items():
        print(f"Processing student index: {student_index}")

    for course_id, course_data in student_indices.items():
        student_indices = course_data.get('student_index', [])
        if isinstance(student_indices, str):
            student_indices = student_indices.split(', ')

        for student_index in student_indices:
            student_index = student_index.strip()
            if not student_index:
                print(f"無効な学生インデックス: {student_index}")
                continue

            student_info = get_firebase_data(f'Students/student_info/{student_index}')
            if not student_info:
                print(f"学生インデックス {student_index} に関連付けられたデータが見つかりませんでした。")
                continue

            student_name = student_info.get('student_name')
            attendance_number = student_info.get('attendance_number')
            if not student_name or not attendance_number:
                print(f"学生インデックス {student_index} の学生名または出席番号が見つかりませんでした。")
                continue

            for month in range(1, 13):
                print(f"Processing month: {month} for student index: {student_index} and course_id: {course_id}")
                requests = prepare_update_requests(course_id, attendance_number, student_name, month, sheets_service, course_id)
                if not requests:
                    print(f"月 {month} のシートを更新するリクエストがありません。")
                    continue

                execute_request_with_retry(sheets_service, course_id, requests)
                print(f"月 {month} のシートを正常に更新しました。")

if __name__ == "__main__":
    main()
