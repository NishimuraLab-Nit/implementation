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

# クラス名からクラスIDと対応するコース名を取得
def get_course_names_from_class(class_name):
    # クラス名からクラスIDを取得
    class_data = get_firebase_data('Students/student_info/student_index')
    if not class_data or not isinstance(class_data, dict):
        print("Firebaseからクラスデータを取得できませんでした。")
        return []

    class_id = None
    for student_index, student_info in class_data.items():
        if student_info.get('class_name') == class_name:
            class_id = student_info.get('class_id')
            break

    if not class_id:
        print(f"指定したクラス名 '{class_name}' に対応するクラスIDが見つかりませんでした。")
        return []

    # クラスIDからコース名を取得
    course_ids = get_firebase_data(f"Students/student_info/student_index/class_id={class_id}/course_id")
    if not course_ids:
        print(f"クラスID {class_id} に関連付けられたコースが見つかりません。")
        return []

    course_names = []
    for cid in course_ids:
        course_data = get_firebase_data(f"Courses/{cid}")
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
    existing_titles = get_all_sheets(sheets_service, spreadsheet_id)
    sheet_title = generate_unique_sheet_title(existing_titles, base_title)

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

    # ユーザーが指定したクラス名
    class_name = input("クラス名を入力してください: ").strip()

    # クラス名からコース名を取得
    course_names = get_course_names_from_class(class_name)
    if not course_names:
        print(f"クラス名 '{class_name}' のコース名が見つかりませんでした。")
        return

    # スプレッドシートIDを指定
    spreadsheet_id = "YOUR_SPREADSHEET_ID"

    for month in range(1, 13):
        print(f"Processing month: {month} for class: {class_name}")
        requests = prepare_update_requests(class_name, course_names, month, 2025, sheets_service, spreadsheet_id)
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
