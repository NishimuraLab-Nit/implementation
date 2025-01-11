from datetime import datetime, timedelta
from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# 定数
FIREBASE_CREDENTIALS_FILE = "firebase-adminsdk.json"
GOOGLE_CREDENTIALS_FILE = "google-credentials.json"
DATABASE_URL = "https://test-51ebc-default-rtdb.firebaseio.com/"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
JAPANESE_WEEKDAYS = ["月", "火", "水", "木", "金", "土", "日"]

# Firebase初期化
def initialize_firebase():
    try:
        firebase_cred = credentials.Certificate(FIREBASE_CREDENTIALS_FILE)
        initialize_app(firebase_cred, {'databaseURL': DATABASE_URL})
        print("Firebase initialized successfully.")
    except Exception as e:
        raise RuntimeError(f"Firebase initialization error: {e}")

# Google Sheets APIサービス初期化
def get_google_sheets_service():
    try:
        google_creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=SCOPES)
        return build('sheets', 'v4', credentials=google_creds)
    except Exception as e:
        raise RuntimeError(f"Google Sheets API initialization error: {e}")

# Firebaseからデータ取得
def get_firebase_data(ref_path):
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Error fetching data from Firebase ({ref_path}): {e}")
        return None

# スプレッドシートの既存シートタイトル取得
def get_existing_sheet_titles(sheets_service, spreadsheet_id):
    try:
        response = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        return [sheet['properties']['title'] for sheet in response.get('sheets', [])]
    except Exception as e:
        print(f"Error fetching existing sheet titles: {e}")
        return []

# シートタイトルからシートIDを取得
def get_sheet_id_by_title(sheets_service, spreadsheet_id, title):
    try:
        response = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in response.get('sheets', []):
            if sheet['properties']['title'] == title:
                return sheet['properties']['sheetId']
        print(f"Sheet with title '{title}' not found.")
        return None
    except Exception as e:
        print(f"Error fetching sheet ID for title '{title}': {e}")
        return None

# 必要な列を追加するリクエスト
def ensure_column_count(sheets_service, spreadsheet_id, sheet_id, required_columns):
    """必要な列数を確保するために列を追加する"""
    try:
        response = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet = next((s for s in response['sheets'] if s['properties']['sheetId'] == sheet_id), None)
        current_columns = sheet['properties']['gridProperties']['columnCount']
        
        if current_columns < required_columns:
            add_columns_request = {
                "appendDimension": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "length": required_columns - current_columns
                }
            }
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [add_columns_request]}
            ).execute()
            print(f"Added {required_columns - current_columns} columns to sheet {sheet_id}.")
    except Exception as e:
        print(f"Error ensuring column count for sheet {sheet_id}: {e}")

# 月ごとのシートを準備
def prepare_monthly_sheets(spreadsheet_id, sheets_service):
    months = [f"{i}月" for i in range(1, 13)]
    existing_titles = set(get_existing_sheet_titles(sheets_service, spreadsheet_id))

    requests = [
        {"addSheet": {"properties": {"title": month}}}
        for month in months if month not in existing_titles
    ]

    if requests:
        try:
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests}
            ).execute()
            print("Monthly sheets created successfully.")
        except Exception as e:
            print(f"Error creating monthly sheets: {e}")
    else:
        print("All monthly sheets already exist.")

# セル更新リクエスト作成
def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

# Google Sheetsの更新リクエスト準備
def prepare_update_requests(sheet_id, class_names, month_index):
    if not class_names:
        print("Class names list is empty.")
        return []

    requests = [create_cell_update_request(sheet_id, 0, 0, "教科")]
    requests.extend(
        create_cell_update_request(sheet_id, i + 1, 0, name) for i, name in enumerate(class_names)
    )

    start_date = datetime(2025, month_index + 1, 1)
    for i in range(31):  # 最大31日分
        date = start_date + timedelta(days=i)
        if date.month != month_index + 1:
            break
        date_string = f"{date.strftime('%m/%d')} ({JAPANESE_WEEKDAYS[date.weekday()]})"
        requests.append(create_cell_update_request(sheet_id, 0, i + 1, date_string))

    return requests

# メイン処理
def main():
    try:
        initialize_firebase()
        sheets_service = get_google_sheets_service()

        # Firebaseから必要なデータを取得
        spreadsheet_id = get_firebase_data('Students/student_info/student_index/E534/sheet_id')
        if not spreadsheet_id:
            raise ValueError("Sheet ID not found in Firebase data.")

        prepare_monthly_sheets(spreadsheet_id, sheets_service)

        class_names = ["数学"]  # 仮設定のクラス名

        for month_index in range(12):
            title = f"{month_index + 1}月"
            sheet_id = get_sheet_id_by_title(sheets_service, spreadsheet_id, title)

            if not sheet_id:
                print(f"Sheet ID for {title} not found. Skipping update.")
                continue

            required_columns = 32  # 最大の日数分の列が必要
            ensure_column_count(sheets_service, spreadsheet_id, sheet_id, required_columns)

            requests = prepare_update_requests(sheet_id, class_names, month_index)
            if not requests:
                print(f"No update requests for {title}.")
                continue

            try:
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': requests}
                ).execute()
                print(f"{title} sheet updated successfully.")
            except Exception as e:
                print(f"Error updating sheet for {title}: {e}")

    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
