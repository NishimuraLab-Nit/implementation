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

# Courses/{course_id}/course_sheet_id から sheet_id を取得
def get_sheet_id(course_id):
    course_data = get_firebase_data(f"Courses/{course_id}")
    if not course_data:
        print(f"コースID {course_id} に対応するデータが見つかりませんでした。")
        return None
    return course_data.get("course_sheet_id")

# Students/enrollment/{course_id}/student_index から学生名を取得
def get_student_names(course_id):
    enrollment_data = get_firebase_data(f"Students/enrollment/course_id/{course_id}")
    if not enrollment_data:
        print(f"コースID {course_id} の登録データが見つかりませんでした。")
        return []

    student_indices = enrollment_data.get("student_index", "").split(",")
    student_names = []

    for student_index in student_indices:
        student_info = get_firebase_data(f"Students/student_info/{student_index.strip()}")
        if student_info and "student_name" in student_info:
            student_names.append(student_info["student_name"])

    return student_names

# シート更新リクエストを準備
def prepare_update_requests(sheet_id, student_names, month, sheets_service, spreadsheet_id, year=2025):
    if not student_names:
        print("学生名リストが空です。Firebaseから取得したデータを確認してください。")
        return []

    base_title = f"{year}-{str(month).zfill(2)}"
    sheet_title = generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title)

    add_sheet_request = create_sheet_request(sheet_title)
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

    requests = []
    requests.append(create_cell_update_request(new_sheet_id, 0, 1, "学生名"))
    requests.append(create_cell_update_request(new_sheet_id, 0, 0, "AN"))

    for i, name in enumerate(student_names):
        requests.append(create_cell_update_request(new_sheet_id, i + 2, 1, name))

    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    current_date = start_date
    start_column = 2
    period_labels = ["1,2限", "3,4限", "5,6限", "7,8限"]
    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = f"{current_date.strftime('%m')}/{current_date.strftime('%d')}"
        requests.append(create_cell_update_request(new_sheet_id, 0, start_column, date_string))
        for period_index, period in enumerate(period_labels):
            requests.append(create_cell_update_request(new_sheet_id, 1, start_column + period_index, period))
        start_column += len(period_labels)
        current_date += timedelta(days=1)

    return requests

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

# シート作成リクエスト
def create_sheet_request(sheet_title):
    return {
        "addSheet": {
            "properties": {"title": sheet_title}
        }
    }

# セル更新リクエストを作成
def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": str(value)}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    class_indices = get_firebase_data("Class/class_index")
    if not class_indices or not isinstance(class_indices, dict):
        print("Classインデックスを取得できませんでした。")
        return

    for class_index, class_data in class_indices.items():
        course_ids = class_data.get("course_id", "").split(",")
        for course_id in course_ids:
            course_id = int(course_id.strip())
            sheet_id = get_sheet_id(course_id)
            if not sheet_id:
                print(f"コースID {course_id} に対応するスプレッドシートIDが見つかりません。")
                continue

            student_names = get_student_names(course_id)
            if not student_names:
                print(f"コースID {course_id} に対応する学生名が見つかりませんでした。")
                continue

            for month in range(1, 13):
                print(f"Processing month: {month} for course ID: {course_id}")
                requests = prepare_update_requests(sheet_id, student_names, month, sheets_service, sheet_id)
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