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
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Firebaseデータ取得エラー: {e}")
        return None

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

# ユニークなシート名を生成
def generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title):
    existing_sheets = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute().get("sheets", [])
    sheet_titles = [sheet["properties"]["title"] for sheet in existing_sheets]
    title = base_title
    counter = 1
    while title in sheet_titles:
        title = f"{base_title} ({counter})"
        counter += 1
    return title

# シート更新リクエストを準備
def prepare_update_requests(sheet_id, student_names, month, sheets_service, spreadsheet_id, year=2025):
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
    response = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'requests': requests}
    ).execute()

    new_sheet_id = next(
        (reply['addSheet']['properties']['sheetId'] for reply in response.get('replies', []) if 'addSheet' in reply),
        None
    )
    if new_sheet_id is None:
        print("新しいシートのIDを取得できませんでした。")
        return []

    # 以降のリクエストに新しいシートIDを使用
     # 以降のリクエストに新しいシートIDを使用
    requests = [
        {"appendDimension": {"sheetId": new_sheet_id, "dimension": "COLUMNS", "length": 125}},
        create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 100),
        create_dimension_request(new_sheet_id, "COLUMNS", 1, 125, 35),
        create_dimension_request(new_sheet_id, "ROWS", 0, 1, 120),
        {"repeatCell": {"range": {"sheetId": new_sheet_id},
                        "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                        "fields": "userEnteredFormat.horizontalAlignment"}},
        {"updateBorders": {"range": {"sheetId": new_sheet_id, "startRowIndex": 0, "endRowIndex": 35, "startColumnIndex": 0,
                                         "endColumnIndex": 125},
                           "top": {"style": "SOLID", "width": 1},
                           "bottom": {"style": "SOLID", "width": 1},
                           "left": {"style": "SOLID", "width": 1},
                           "right": {"style": "SOLID", "width": 1}}},
        {"setBasicFilter": {"filter": {"range": {"sheetId": new_sheet_id, "startRowIndex": 0, "endRowIndex": 35,
                                                     "startColumnIndex": 0, "endColumnIndex": 125}}}}
                                                     ]

    # 学生名を記載
    requests.append(create_cell_update_request(new_sheet_id, 2, 0, "学生名"))
    for i, name in enumerate(student_names):
        requests.append(create_cell_update_request(new_sheet_id, i + 3, 0, name))

    # 日付と授業時限を設定
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    current_date = start_date
    start_column = 1  # 日付の開始列（1列目は学生名用）
    period_labels = ["1,2限", "3,4限", "5,6限", "7,8限"]  # 授業時限ラベル

    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = f"{current_date.strftime('%m')}/{current_date.strftime('%d')}\n{japanese_weekdays[weekday]}"
        requests.append(create_cell_update_request(new_sheet_id, 0, start_column, date_string))

        # 授業時限を記載（1列ごとに1つの時限）
        for period_index, period in enumerate(period_labels):
            requests.append(create_cell_update_request(new_sheet_id, 1, start_column + period_index, period))

        # 土日のセルに色を付ける
        if weekday == 5:  # 土曜日
            color = {"red": 0.8, "green": 0.9, "blue": 1.0}  # 青色
            requests.append(create_weekend_color_request(new_sheet_id, 0, 35, start_column, start_column + len(period_labels), color))
        elif weekday == 6:  # 日曜日
            color = {"red": 1.0, "green": 0.8, "blue": 0.8}  # 赤色
            requests.append(create_weekend_color_request(new_sheet_id, 0, 35, start_column, start_column + len(period_labels), color))

        # 日付ごとに時限分の列を進める
        start_column += len(period_labels)
        current_date += timedelta(days=1)

    return requests

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # クラスデータを取得
    class_indices = get_firebase_data('Class/class_index')
    if not class_indices or not isinstance(class_indices, dict):
        print("Classインデックスを取得できませんでした。")
        return

    for class_index, class_data in class_indices.items():
        spreadsheet_id = class_data.get("class_sheet_id")
        if not spreadsheet_id:
            print(f"クラス {class_index} のスプレッドシートIDが見つかりません。")
            continue

        # 学生データを取得
        student_indices = get_firebase_data('Students/student_info/student_index')
        if not student_indices or not isinstance(student_indices, dict):
            print("学生インデックスを取得できませんでした。")
            continue

        # クラスに一致する学生を取得
        student_names = [
            student_data.get("student_name")
            for index, student_data in student_indices.items()
            if str(index).startswith(class_index) and student_data.get("student_name")
        ]

        if not student_names:
            print(f"クラス {class_index} に一致する学生名が見つかりませんでした。")
            continue

        # 各月のシートを更新
        for month in range(1, 13):
            print(f"Processing month: {month} for class index: {class_index}")
            requests = prepare_update_requests(class_index, student_names, month, sheets_service, spreadsheet_id)
            if not requests:
                print(f"月 {month} のシートを更新するリクエストがありません。")
                continue

            # シートを更新
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            ).execute()
            print(f"月 {month} のシートを正常に更新しました。")

if __name__ == "__main__":
    main()
