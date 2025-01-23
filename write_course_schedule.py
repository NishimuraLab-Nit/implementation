from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
from firebase_admin import credentials, initialize_app, db

# Firebase初期化
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

# シート作成リクエスト
def create_sheet_request(sheet_title):
    return {
        "addSheet": {
            "properties": {
                "title": sheet_title,
                "gridProperties": {"rowCount": 1000, "columnCount": 33}
            }
        }
    }

# 列幅・行高設定リクエスト
def create_dimension_request(sheet_id, dimension, start_index, end_index, pixel_size):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": dimension, "startIndex": start_index, "endIndex": end_index},
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize"
        }
    }

# セルの値更新リクエスト
def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

# 土日色付けリクエスト
def create_weekend_color_request(sheet_id, start_row, end_row, start_col, end_col, color):
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                "startColumnIndex": start_col, "endColumnIndex": end_col
            },
            "cell": {"userEnteredFormat": {"backgroundColor": color}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    }

# シート設定リクエスト準備
def prepare_sheet_requests(sheet_title, sheets_service, spreadsheet_id, student_indices, year=2025, month=1):
    # シート作成リクエスト
    add_sheet_request = create_sheet_request(sheet_title)
    response = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [add_sheet_request]}
    ).execute()

    # 新しいシートIDを取得
    new_sheet_id = next(
        (reply['addSheet']['properties']['sheetId'] for reply in response.get('replies', [])
         if 'addSheet' in reply), None)
    if not new_sheet_id:
        print("新しいシートのIDを取得できませんでした。")
        return []

    # 列幅・行高の設定
    requests = [
        create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 100),  # 1列目の幅 (attendance_number)
        create_dimension_request(new_sheet_id, "COLUMNS", 1, 2, 150),  # 2列目の幅 (student_name)
        create_dimension_request(new_sheet_id, "COLUMNS", 2, 33, 100),  # 3列目以降 (日付列)
        create_dimension_request(new_sheet_id, "ROWS", 0, 1, 120),  # 1行目の高さ
        create_cell_update_request(new_sheet_id, 0, 0, "AN"),  # 1行1列目 (AN)
        create_cell_update_request(new_sheet_id, 0, 1, "Student Name"),  # 1行2列目 (Student Name)
    ]

    # 各学生の attendance_number と名前を設定
    for row_index, (student_index, student_data) in enumerate(student_indices.items(), start=1):
        attendance_number = student_data.get(f"{student_index}/attendance_number", "")
        student_name = student_data.get("student_name", "")
        requests.append(create_cell_update_request(new_sheet_id, row_index, 0, str(attendance_number)))  # AN列
        requests.append(create_cell_update_request(new_sheet_id, row_index, 1, student_name))  # Student Name列

    # 日付列の設定と土日の色付け
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    current_date = start_date
    while current_date <= end_date:
        col_index = current_date.day + 1  # 3列目から始まる
        weekday = current_date.weekday()
        date_string = current_date.strftime('%m/%d')  # "MM/DD"形式
        requests.append(create_cell_update_request(new_sheet_id, 0, col_index, date_string))

        # 土日の背景色設定
        if weekday == 5:  # 土曜日
            requests.append(create_weekend_color_request(new_sheet_id, 1, 1000, col_index, col_index + 1,
                                                         {"red": 0.8, "green": 0.9, "blue": 1.0}))
        elif weekday == 6:  # 日曜日
            requests.append(create_weekend_color_request(new_sheet_id, 1, 1000, col_index, col_index + 1,
                                                         {"red": 1.0, "green": 0.8, "blue": 0.8}))

        current_date += timedelta(days=1)

    return requests

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()
    spreadsheet_id = "your-spreadsheet-id"  # 使用するスプレッドシートID

    # Firebaseから学生データを取得
    student_indices = get_firebase_data("Students/student_info/student_index")
    if not student_indices or not isinstance(student_indices, dict):
        print("学生データが取得できませんでした。")
        return

    # シート設定リクエストを準備
    year = 2025
    month = 1
    sheet_title = f"{year}-{str(month).zfill(2)}"
    requests = prepare_sheet_requests(sheet_title, sheets_service, spreadsheet_id, student_indices, year, month)
    if not requests:
        print("シート設定リクエストが作成されませんでした。")
        return

    # Google Sheetsを更新
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests}
    ).execute()
    print(f"シート {sheet_title} が正常に設定されました。")

if __name__ == "__main__":
    main()
