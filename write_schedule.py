from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
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
    return build('sheets', 'v4', credentials=google_creds)

# Firebaseからデータを取得
def get_firebase_data(ref_path):
    return db.reference(ref_path).get()

# セル更新リクエストを作成
def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

# 列や行のプロパティ設定リクエストを作成
def create_dimension_request(sheet_id, dimension, start_index, end_index, pixel_size):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": dimension, "startIndex": start_index, "endIndex": end_index},
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize"
        }
    }

# 黒背景リクエストを作成
def create_black_background_request(sheet_id, start_row, end_row, start_col, end_col):
    black_color = {"red": 0.0, "green": 0.0, "blue": 0.0, "alpha": 1.0}
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": black_color
                }
            },
            "fields": "userEnteredFormat.backgroundColor"
        }
    }

# 新しいシート作成リクエストを作成する関数
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

# Google Sheetsのシートをすべて取得
def get_all_sheets(sheets_service, spreadsheet_id):
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet.get('sheets', [])
    return [sheet['properties']['title'] for sheet in sheets]

# シート名の重複を避けるためにユニークな名前を生成
def generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title):
    existing_titles = get_all_sheets(sheets_service, spreadsheet_id)
    if base_title not in existing_titles:
        return base_title

    index = 1
    while f"{base_title}-{index}" in existing_titles:
        index += 1
    return f"{base_title}-{index}"

# Firebaseからデータを取得してコース名を生成する関数
def get_course_names_from_class(class_name):
    # class_nameに対応するclass_idを取得
    classes = get_firebase_data('Classes')
    if not classes or class_name not in classes:
        print(f"Class name '{class_name}' が見つかりませんでした。")
        return []

    class_id = classes[class_name].get('class_id')
    if not class_id:
        print(f"Class name '{class_name}' に対応するclass_idが見つかりません。")
        return []

    # class_idに対応するcourse_nameを取得
    course_ids = get_firebase_data(f'Students/enrollment/class_id/{class_id}/course_id')
    if not course_ids:
        print(f"Class ID '{class_id}' に対応するコースが見つかりません。")
        return []

    course_names = []
    for course_id in course_ids:
        course_data = get_firebase_data(f'Courses/{course_id}')
        if course_data:
            course_name = course_data.get('course_name')
            if course_name:
                course_names.append(course_name)

    return course_names

# シート更新リクエストを準備
def prepare_update_requests(sheet_id, course_names, month, year, sheets_service, spreadsheet_id):
    if not course_names:
        print("コース名リストが空です。Firebaseから取得したデータを確認してください。")
        return []

    # ユニークなシート名を生成
    base_title = f"{year}-{str(month).zfill(2)}"
    sheet_title = generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title)

    # 新しいシートを作成するリクエスト
    add_sheet_request = create_sheet_request(sheet_title)
    requests = [add_sheet_request]

    # 新しいシートのIDを取得
    response = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'requests': requests}
    ).execute()

    new_sheet_id = None
    for reply in response.get('replies', []):
        if 'addSheet' in reply:
            new_sheet_id = reply['addSheet']['properties']['sheetId']

    if new_sheet_id is None:
        print("新しいシートのIDを取得できませんでした。")
        return []

    # その他の更新リクエストを構築
    requests = [
        create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 100),
        create_dimension_request(new_sheet_id, "COLUMNS", 1, 32, 35),
        create_dimension_request(new_sheet_id, "ROWS", 0, 1, 120),
        {"repeatCell": {"range": {"sheetId": new_sheet_id},
                        "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                        "fields": "userEnteredFormat.horizontalAlignment"}}
    ]

    # 教科名を設定
    requests.append(create_cell_update_request(new_sheet_id, 0, 0, "教科"))
    for i, name in enumerate(course_names):
        requests.append(create_cell_update_request(new_sheet_id, i + 1, 0, name))

    # 黒背景を適用
    requests.append(create_black_background_request(new_sheet_id, 1, len(course_names) + 1, 1, 32))

    # 日付と土日セルの色付け
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    current_date = start_date
    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = f"{current_date.strftime('%m/%d')} ({['月', '火', '水', '木', '金', '土', '日'][weekday]})"
        col_index = (current_date.day - 1) + 1
        requests.append(create_cell_update_request(new_sheet_id, 0, col_index, date_string))
        current_date += timedelta(days=1)

    return requests

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    class_name = input("Enter class name: ")
    course_names = get_course_names_from_class(class_name)
    if not course_names:
        print(f"Class name '{class_name}' に関連付けられたコース名が見つかりませんでした。")
        return

    spreadsheet_id = input("Enter the Google Spreadsheet ID: ")
    for month in range(1, 13):
        print(f"Processing month: {month} for class: {class_name}")
        requests = prepare_update_requests(None, course_names, month, 2025, sheets_service, spreadsheet_id)
        if not requests:
            print(f"月 {month} のシートを更新するリクエストがありません。")
            continue

        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        ).execute()
        print(f"月 {month} のシートを正常に更新しました。")

if __name__ == "__main__":
    main()
