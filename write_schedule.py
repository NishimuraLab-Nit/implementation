from datetime import datetime, timedelta
from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

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
                print(f"Found sheet ID: {sheet['properties']['sheetId']} for title: {title}")
                return sheet['properties']['sheetId']
        print(f"Sheet with title '{title}' not found.")
        return None
    except Exception as e:
        print(f"Error fetching sheet ID for title '{title}': {e}")
        return None

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

# Google Sheetsの更新リクエストを送信
def update_sheet_with_requests(sheets_service, spreadsheet_id, sheet_title, updates):
    try:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_title}'!A1",
            valueInputOption="RAW",
            body={"values": updates}
        ).execute()
        print(f"Sheet '{sheet_title}' updated successfully.")
    except HttpError as e:
        print(f"HttpError occurred while updating sheet '{sheet_title}': {e.content}")
    except Exception as e:
        print(f"Error updating sheet '{sheet_title}': {e}")

# シート更新内容を準備
def prepare_update_requests(class_names, month_index):
    if not class_names:
        print("Class names list is empty.")
        return []

    updates = [["教科"] + [f"Day {i + 1}" for i in range(31)]]  # ヘッダー

    start_date = datetime(2025, month_index + 1, 1)
    dates_row = []
    for i in range(31):  # 最大31日分
        date = start_date + timedelta(days=i)
        if date.month != month_index + 1:
            break
        date_string = f"{date.strftime('%m/%d')} ({JAPANESE_WEEKDAYS[date.weekday()]})"
        dates_row.append(date_string)

    # 更新内容
    updates.append([""] + dates_row)  # 空セル + 日付
    for name in class_names:
        updates.append([name] + [""] * 31)

    return updates

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

        # クラス名（仮設定）
        class_names = ["数学", "英語", "物理"]

        # 各月のシートを更新
        for month_index in range(12):  # 1月～12月
            title = f"{month_index + 1}月"
            sheet_id = get_sheet_id_by_title(sheets_service, spreadsheet_id, title)

            if not sheet_id:
                print(f"Sheet ID for {title} not found. Skipping update.")
                continue

            updates = prepare_update_requests(class_names, month_index)
            if not updates:
                print(f"No updates prepared for {title}.")
                continue

            update_sheet_with_requests(sheets_service, spreadsheet_id, title, updates)

    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
