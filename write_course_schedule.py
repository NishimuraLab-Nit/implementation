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

# コースごとの学生データを取得
def get_student_data(course_id):
    # Enrollmentからstudent_indexを取得
    enrollment_data = get_firebase_data(f'Students/enrollment/course_id/{course_id}/student_index')
    if not enrollment_data:
        print(f"コース {course_id} の学生インデックスを取得できませんでした。")
        return []

    student_indices = [index.strip() for index in enrollment_data.split(',')]
    
    # student_nameを取得
    student_names = []
    for student_index in student_indices:
        student_info = get_firebase_data(f'Students/student_info/{student_index}')
        if student_info and 'student_name' in student_info:
            student_names.append(student_info['student_name'])

    return student_names

# コースのシートIDを取得
def get_course_sheet_id(course_id):
    course_data = get_firebase_data(f'Courses/{course_id}')
    if course_data and 'course_sheet_id' in course_data:
        return course_data['course_sheet_id']
    print(f"コース {course_id} のシートIDが見つかりませんでした。")
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
def prepare_update_requests(course_id, student_names, month, sheets_service, spreadsheet_id, year=2025):
    if not student_names:
        print("学生名リストが空です。Firebaseから取得したデータを確認してください。")
        return []

    # ユニークなシート名を生成
    base_title = f"{year}-{str(month).zfill(2)}"
    sheet_title = generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title)

    # シートを追加するリクエスト
    add_sheet_request = create_sheet_request(sheet_title)
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

    # 必要な列をスプレッドシートに追加
    requests = [
        {"appendDimension": {"sheetId": new_sheet_id, "dimension": "COLUMNS", "length": 126}},
        create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 35),
        create_dimension_request(new_sheet_id, "COLUMNS", 1, 1, 100),
        create_dimension_request(new_sheet_id, "COLUMNS", 2, 126, 35),
        create_dimension_request(new_sheet_id, "ROWS", 0, 1, 120),
        {"repeatCell": {"range": {"sheetId": new_sheet_id},
                        "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                        "fields": "userEnteredFormat.horizontalAlignment"}},
        {"updateBorders": {"range": {"sheetId": new_sheet_id, "startRowIndex": 0, "endRowIndex": 35, "startColumnIndex": 0,
                                         "endColumnIndex": 126},
                           "top": {"style": "SOLID", "width": 1},
                           "bottom": {"style": "SOLID", "width": 1},
                           "left": {"style": "SOLID", "width": 1},
                           "right": {"style": "SOLID", "width": 1}}},
        {"setBasicFilter": {"filter": {"range": {"sheetId": new_sheet_id, "startRowIndex": 0, "endRowIndex": 35,
                                                     "startColumnIndex": 0, "endColumnIndex": 126}}}}
    ]

    # 学生名を記載
    requests.append(create_cell_update_request(new_sheet_id, 0, 1, "学生名"))
    requests.append(create_cell_update_request(new_sheet_id, 0, 0, "AN"))

    for i, name in enumerate(student_names):
        requests.append(create_cell_update_request(new_sheet_id, i + 2, 1, name))

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

        if weekday == 5:
            color = {"red": 0.8, "green": 0.9, "blue": 1.0}
            requests.append(create_weekend_color_request(new_sheet_id, 0, 35, start_column, start_column + len(period_labels), color))
        elif weekday == 6:
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

    # Coursesから全てのコースIDを取得
    courses = get_firebase_data('Courses')
    if not courses or not isinstance(courses, dict):
        print("コースデータを取得できませんでした。")
        return

    for course_id, course_data in courses.items():
        if not course_id.isdigit():
            continue  # コースIDが数値でない場合はスキップ

        course_id = int(course_id)  # コースIDを整数に変換
        spreadsheet_id = get_course_sheet_id(course_id)
        if not spreadsheet_id:
            continue

        student_names = get_student_data(course_id)
        if not student_names:
            print(f"コース {course_id} に一致する学生名が見つかりませんでした。")
            continue

        for month in range(1, 13):
            print(f"Processing month: {month} for course ID: {course_id}")
            # 必要なリクエストを準備
            requests = prepare_update_requests(course_id, student_names, month, sheets_service, spreadsheet_id)
            if not requests:
                print(f"月 {month} のシートを更新するリクエストがありません。")
                continue

            # リクエストを送信
            execute_with_retry(
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': requests}
                )
            )
            print(f"月 {month} のシートを正常に更新しました。")

if __name__ == "__main__":
    main()
