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

# 列数を拡張するリクエスト
def ensure_sheet_columns(sheet_id, new_sheet_id, required_columns):
    return {
        "appendDimension": {
            "sheetId": new_sheet_id,
            "dimension": "COLUMNS",
            "length": required_columns
        }
    }

# シート全体の背景色を設定
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

    # ヘッダー行の背景色を設定 (薄い灰色)
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

# 曜日による背景色の設定 (土曜日: 青, 日曜日: 赤)
def add_date_colors(sheet_id, start_column, total_days, new_sheet_id):
    requests = []
    for day in range(total_days):
        weekday = (day + 4) % 7  # 曜日を計算 (例: 2025年1月1日は水曜日)
        if weekday == 5:  # 土曜日
            color = {"red": 0.8, "green": 0.9, "blue": 1.0}  # 青
        elif weekday == 6:  # 日曜日
            color = {"red": 1.0, "green": 0.8, "blue": 0.8}  # 赤
        else:
            color = None

        if color:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": new_sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": start_column + day,
                        "endColumnIndex": start_column + day + 1
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": color}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

    return requests

# 日付と曜日を記入
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

    # 曜日による色付けを追加
    requests.extend(add_date_colors(sheet_id, 2, total_days, new_sheet_id))

    return requests

# シート更新リクエストを準備
def prepare_update_requests(sheet_id, student_names, attendance_numbers, month, sheets_service, spreadsheet_id, year=2025):
    if not student_names:
        print("学生名リストが空です。")
        return []

    base_title = f"{year}-{str(month).zfill(2)}"
    add_sheet_request = {
        "addSheet": {"properties": {"title": base_title}}
    }
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
    if new_sheet_id is None:
        print("新しいシートのIDを取得できませんでした。")
        return []

    requests = [
    {"appendDimension": {"sheetId": new_sheet_id, "dimension": "COLUMNS", "length": 126}},  # 必要な列を確保
    # 列幅の設定
    create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 50),  # 出席番号列の幅
    create_dimension_request(new_sheet_id, "COLUMNS", 1, 2, 150),  # 学生名列の幅
    create_dimension_request(new_sheet_id, "COLUMNS", 2, 126, 80),  # 日付列の幅
    # 行高さの設定
    create_dimension_request(new_sheet_id, "ROWS", 0, 1, 40),  # ヘッダー行の高さ
    create_dimension_request(new_sheet_id, "ROWS", 1, 35, 30),  # 学生データ行の高さ
    # ヘッダー行の背景色
    {"repeatCell": {
        "range": {"sheetId": new_sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 126},
        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}}},
        "fields": "userEnteredFormat.backgroundColor"
    }},
    # テキストの中央揃え
    {"repeatCell": {
        "range": {"sheetId": new_sheet_id, "startRowIndex": 0, "endRowIndex": 35, "startColumnIndex": 0, "endColumnIndex": 126},
        "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
        "fields": "userEnteredFormat.horizontalAlignment"
    }},
    # セルの境界線を設定
    {"updateBorders": {
        "range": {"sheetId": new_sheet_id, "startRowIndex": 0, "endRowIndex": 35, "startColumnIndex": 0, "endColumnIndex": 126},
        "top": {"style": "SOLID", "width": 1},
        "bottom": {"style": "SOLID", "width": 1},
        "left": {"style": "SOLID", "width": 1},
        "right": {"style": "SOLID", "width": 1}
    }},
    # フィルタを適用
    {"setBasicFilter": {
        "filter": {"range": {"sheetId": new_sheet_id, "startRowIndex": 0, "endRowIndex": 35, "startColumnIndex": 0, "endColumnIndex": 126}}
    }}
]

# 学生名と出席番号を記載
requests.append(create_cell_update_request(new_sheet_id, 0, 1, "学生名"))
requests.append(create_cell_update_request(new_sheet_id, 0, 0, "AN"))

for i, (name, attendance_number) in enumerate(zip(student_names, attendance_numbers)):
    requests.append(create_cell_update_request(new_sheet_id, i + 2, 1, name))  # 学生名
    requests.append(create_cell_update_request(new_sheet_id, i + 2, 0, attendance_number))  # 出席番号

# 日付と授業時限を設定
japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
start_date = datetime(year, month, 1)
end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

current_date = start_date
start_column = 2
period_labels = ["1,2限", "3,4限", "5,6限", "7,8限"]

while current_date <= end_date:
    weekday = current_date.weekday()
    date_string = f"{current_date.strftime('%m')}\n月\n{current_date.strftime('%d')}\n日\n⌢\n{japanese_weekdays[weekday]}\n⌣"
    requests.append(create_cell_update_request(new_sheet_id, 0, start_column, date_string))

    for period_index, period in enumerate(period_labels):
        requests.append(create_cell_update_request(new_sheet_id, 1, start_column + period_index, period))

    # 土曜日と日曜日に背景色を設定
    if weekday == 5:  # 土曜日
        color = {"red": 0.8, "green": 0.9, "blue": 1.0}
        requests.append(create_weekend_color_request(new_sheet_id, 0, 35, start_column, start_column + len(period_labels), color))
    elif weekday == 6:  # 日曜日
        color = {"red": 1.0, "green": 0.8, "blue": 0.8}
        requests.append(create_weekend_color_request(new_sheet_id, 0, 35, start_column, start_column + len(period_labels), color))

    start_column += len(period_labels)
    current_date += timedelta(days=1)

# 残りのシートの背景色を黒に設定
requests.append(create_black_background_request(new_sheet_id, 35, 1000, 0, 1000))
requests.append(create_black_background_request(new_sheet_id, 0, 1000, 126, 1000))

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
