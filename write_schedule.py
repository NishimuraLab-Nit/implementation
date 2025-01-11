from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta

def initialize_firebase():
    """Firebaseの初期化"""
    try:
        firebase_cred = credentials.Certificate("firebase-adminsdk.json")
        initialize_app(firebase_cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })
        print("Firebase initialized successfully.")
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        raise

def get_google_sheets_service():
    """Google Sheets APIのサービスを取得"""
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
        return build('sheets', 'v4', credentials=google_creds)
    except Exception as e:
        print(f"Error initializing Google Sheets API: {e}")
        raise

def get_firebase_data(ref_path):
    """Firebaseからデータを取得"""
    try:
        data = db.reference(ref_path).get()
        print(f"Data fetched from Firebase ({ref_path}): {data}")
        return data
    except Exception as e:
        print(f"Error fetching data from Firebase ({ref_path}): {e}")
        return None

def get_existing_sheet_titles(sheets_service, spreadsheet_id):
    """既存のシートタイトルを取得"""
    try:
        response = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = response.get('sheets', [])
        titles = [sheet['properties']['title'] for sheet in sheets]
        return titles
    except Exception as e:
        print(f"Error fetching existing sheet titles: {e}")
        return []

def get_sheet_id_by_title(sheets_service, spreadsheet_id, title):
    """指定したタイトルに一致するシートのIDを取得"""
    try:
        response = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = response.get('sheets', [])
        for sheet in sheets:
            if sheet['properties']['title'] == title:
                return sheet['properties']['sheetId']
        print(f"Sheet with title '{title}' not found.")
        return None
    except Exception as e:
        print(f"Error fetching sheet ID by title '{title}': {e}")
        return None

def prepare_monthly_sheets(spreadsheet_id, sheets_service):
    """1月～12月のシートを作成"""
    months = [f"{i}月" for i in range(1, 13)]
    existing_titles = get_existing_sheet_titles(sheets_service, spreadsheet_id)

    requests = [
        {"addSheet": {"properties": {"title": month}}}
        for month in months if month not in existing_titles
    ]

    if not requests:
        print("All monthly sheets already exist. No new sheets added.")
        return

    try:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()
        print("Monthly sheets created successfully.")
    except Exception as e:
        print(f"Error creating monthly sheets: {e}")

def create_cell_update_request(sheet_id, row_index, column_index, value):
    """Google Sheetsのセル更新リクエストを作成"""
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

def prepare_update_requests(sheet_id, class_names, month_index):
    """Google Sheets更新用リクエストを準備"""
    if not class_names:
        print("Class names list is empty after processing. Check data integrity.")
        return []

    requests = []

    # 教科名を追加
    requests.append(create_cell_update_request(sheet_id, 0, 0, "教科"))
    requests.extend(create_cell_update_request(sheet_id, i + 1, 0, name) for i, name in enumerate(class_names))

    # 日付を追加
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(2025, month_index + 1, 1)

    for i in range(31):  # 最大31日分のデータ
        date = start_date + timedelta(days=i)
        if date.month != month_index + 1:  # 月が変わったら終了
            break
        weekday = date.weekday()
        date_string = f"{date.strftime('%m/%d')} ({japanese_weekdays[weekday]})"
        requests.append(create_cell_update_request(sheet_id, 0, i + 1, date_string))

    return requests

def main():
    """メイン関数"""
    try:
        initialize_firebase()
        sheets_service = get_google_sheets_service()

        # Firebaseから必要なデータを取得
        sheet_id = get_firebase_data('Students/student_info/student_index/E534/sheet_id')
        student_course_ids = get_firebase_data('Students/enrollment/student_index/E534/course_id')
        courses = get_firebase_data('Courses/course_id')

        if not sheet_id:
            raise ValueError("Sheet ID not found in Firebase data.")

        # 1月～12月のシート作成
        prepare_monthly_sheets(sheet_id, sheets_service)

        # 各月のデータを更新
        for month_index in range(12):  # 1月～12月
            title = f"{month_index + 1}月"
            sheet_id_by_title = get_sheet_id_by_title(sheets_service, sheet_id, title)

            if not sheet_id_by_title:
                print(f"Sheet ID for {title} not found. Skipping update.")
                continue

            class_names = ["数学"]  # 仮のクラス名リストを設定
            requests = prepare_update_requests(sheet_id_by_title, class_names, month_index)

            if not requests:
                print(f"No update requests for {title}.")
                continue

            try:
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={'requests': requests}
                ).execute()
                print(f"{title} sheet updated successfully.")
            except Exception as e:
                print(f"Error updating sheet for {title}: {e}")

    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
