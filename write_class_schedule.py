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

    # 実際のbatchUpdateで新しいシートのIDを取得
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

    # シートの初期設定
    requests = [
        {"appendDimension": {"sheetId": new_sheet_id, "dimension": "COLUMNS", "length": 100}},
        create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 100),
        create_dimension_request(new_sheet_id, "ROWS", 0, 1, 120),
        {"repeatCell": {"range": {"sheetId": new_sheet_id},
                        "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                        "fields": "userEnteredFormat.horizontalAlignment"}}
    ]

    # 学生名を3行目から記載
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
    period_index = 0  # 授業時限ラベルのインデックス

    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = f"{current_date.strftime('%m')}/{current_date.strftime('%d')}\n{japanese_weekdays[weekday]}"
        date_column = start_column

        # 日付を記載
        requests.append(create_cell_update_request(new_sheet_id, 0, date_column, date_string))

        # 授業時限を記載（3列ごとに1つの時限）
        for i in range(3):
            requests.append(create_cell_update_request(new_sheet_id, 1, date_column + i, period_labels[period_index]))
        period_index = (period_index + 1) % len(period_labels)

        # 日付ごとに3列空ける
        start_column += 3
        current_date += timedelta(days=1)

    return requests

def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # class_indexを取得
    class_indices = get_firebase_data('Class/class_index')
    if not class_indices or not isinstance(class_indices, dict):
        print("Classインデックスを取得できませんでした。")
        return

    for class_index, class_data in class_indices.items():
        print(f"Processing class index: {class_index}")

        # シートIDを取得
        file_name = class_data.get('file_name')
        if not file_name:
            print(f"Classインデックス {class_index} のfile_nameが見つかりません。")
            continue

        # 学生データを取得
        student_indices = get_firebase_data('Students/student_info/student_index')
        if not student_indices or not isinstance(student_indices, dict):
            print("学生インデックスを取得できませんでした。")
            continue

        # class_indexと一致するstudent_indexだけを抽出
        matching_student_indices = [
            index for index in student_indices if str(index).startswith(class_index)
        ]

        # 学生名を取得
        student_names = []
        for student_index in matching_student_indices:
            student_data = student_indices.get(student_index)
            if not student_data:
                print(f"学生インデックス {student_index} のデータが見つかりません。")
                continue

            student_name = student_data.get("student_name")
            if student_name:
                student_names.append(student_name)

        if not student_names:
            print(f"Classインデックス {class_index} に一致する学生名が見つかりませんでした。")
            continue

        # 各月のシートを更新
        for month in range(1, 13):
            print(f"Processing month: {month} for class index: {class_index}")
            requests = prepare_update_requests(file_name, student_names, month, sheets_service, file_name)
            if not requests:
                print(f"月 {month} のシートを更新するリクエストがありません。")
                continue

            # シートを更新
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=file_name,
                body={'requests': requests}
            ).execute()
            print(f"月 {month} のシートを正常に更新しました。")

if __name__ == "__main__":
    main()
