from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from google_auth_httplib2 import AuthorizedHttp
import httplib2
from datetime import datetime, timedelta
import time

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
        except Exception as e:
            print(f"リクエスト失敗 ({attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise

# セル更新リクエストを作成
def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
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

# 学生名やデータをスプレッドシートに記載
def setup_student_data(sheet_id, student_names, attendance_numbers):
    requests = []
    # ヘッダー行の作成
    requests.append(create_cell_update_request(sheet_id, 0, 0, "AN"))
    requests.append(create_cell_update_request(sheet_id, 0, 1, "学生名"))

    # 学生情報の挿入
    for i, (name, number) in enumerate(zip(student_names, attendance_numbers)):
        requests.append(create_cell_update_request(sheet_id, i + 1, 0, str(number)))
        requests.append(create_cell_update_request(sheet_id, i + 1, 1, name))
    
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

# シート更新リクエストを準備
def prepare_update_requests(sheet_id, student_names, attendance_numbers):
    requests = setup_student_data(sheet_id, student_names, attendance_numbers)
    # 必要に応じて他のリクエストを追加
    return requests

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    class_indices = get_firebase_data('Class/class_index')
    if not class_indices or not isinstance(class_indices, dict):
        print("Classインデックスを取得できませんでした。")
        return

    for class_index, class_data in class_indices.items():
        spreadsheet_id = class_data.get("class_sheet_id")
        if not spreadsheet_id:
            print(f"クラス {class_index} のスプレッドシートIDが見つかりません。")
            continue

        student_indices = get_firebase_data('Students/student_info/student_index')
        if not student_indices or not isinstance(student_indices, dict):
            print("学生インデックスを取得できませんでした。")
            continue

        student_names = [
            student_data.get("student_name")
            for index, student_data in student_indices.items()
            if str(index).startswith(class_index) and student_data.get("student_name")
        ]

        attendance_numbers = [
            student_data.get("attendance_number")
            for index, student_data in student_indices.items()
            if str(index).startswith(class_index) and student_data.get("attendance_number")
        ]

        if not student_names:
            print(f"クラス {class_index} に一致する学生名が見つかりませんでした。")
            continue

        for month in range(1, 13):
            print(f"Processing month: {month} for class index: {class_index}")
            base_title = f"Attendance-{month}"
            sheet_title = generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title)

            # シートを追加するリクエスト
            add_sheet_request = create_sheet_request(sheet_title)
            response = execute_with_retry(
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': [add_sheet_request]}
                )
            )

            # 新しいシートのIDを取得
            new_sheet_id = next(
                reply['addSheet']['properties']['sheetId']
                for reply in response.get("replies", [])
                if "addSheet" in reply
            )

            # シートのデータを設定するリクエストを準備
            update_requests = prepare_update_requests(new_sheet_id, student_names, attendance_numbers)

            # リクエストを実行
            execute_with_retry(
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': update_requests}
                )
            )
            print(f"月 {month} のシートを正常に更新しました。")

if __name__ == "__main__":
    main()
