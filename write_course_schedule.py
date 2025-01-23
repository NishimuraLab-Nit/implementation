from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta

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

# Firebaseからデータを取得
def get_firebase_data(ref_path):
    return db.reference(ref_path).get()

# 学生のコースIDを取得
def get_student_course_ids(student_index):
    student_data = get_firebase_data(f"Students/enrollment/student_index/{student_index}")
    print(f"学生インデックス {student_index} のデータ: {student_data}")  # デバッグ用
    if not student_data:
        print(f"学生インデックス {student_index} のデータが見つかりません。")
        return []
    course_ids = student_data.get("course_id", "")
    print(f"取得したコースID: {course_ids}")  # デバッグ用
    return [course.strip() for course in course_ids.split(",") if course.strip()]

# 新しいシートを作成するリクエスト
def create_sheet_request(sheet_title):
    return {
        "addSheet": {
            "properties": {
                "title": sheet_title,
                "gridProperties": {"rowCount": 1000, "columnCount": 32}
            }
        }
    }

# セルの値を更新するリクエスト
def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

# 列や行のプロパティ更新リクエスト
def create_dimension_request(sheet_id, dimension, start_index, end_index, pixel_size):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": dimension, "startIndex": start_index, "endIndex": end_index},
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize"
        }
    }

# 背景色を黒に設定するリクエスト
def create_black_background_request(sheet_id, start_row, end_row, start_col, end_col):
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                "startColumnIndex": start_col, "endColumnIndex": end_col
            },
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    }

# 日付セルや土日の背景色設定を含むリクエスト作成
def prepare_update_requests(sheet_id, student_name, month, sheets_service, spreadsheet_id, year=2025):
    base_title = f"{year}-{str(month).zfill(2)}"
    sheet_title = base_title

    # シートの作成リクエスト
    requests = [create_sheet_request(sheet_title)]

    # バッチリクエストを送信して新しいシートIDを取得
    response = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests}
    ).execute()

    new_sheet_id = next((reply['addSheet']['properties']['sheetId'] for reply in response.get('replies', [])
                         if 'addSheet' in reply), None)
    if not new_sheet_id:
        print("新しいシートのIDを取得できませんでした。")
        return []

    # 更新リクエストの準備
    requests = [
        create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 100),
        create_dimension_request(new_sheet_id, "COLUMNS", 1, 32, 35),
        create_dimension_request(new_sheet_id, "ROWS", 0, 1, 120),
        create_cell_update_request(new_sheet_id, 0, 0, "学生名"),
        create_cell_update_request(new_sheet_id, 1, 0, student_name)
    ]

    # 日付と土日セルの色付け
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    current_date = start_date
    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = f"{current_date.strftime('%m')}月\n{current_date.strftime('%d')}日\n{japanese_weekdays[weekday]}"
        requests.append(create_cell_update_request(new_sheet_id, 0, current_date.day, date_string))

        # 土曜と日曜の色付け
        if weekday in (5, 6):
            color = {"red": 0.8, "green": 0.9, "blue": 1.0} if weekday == 5 else {"red": 1.0, "green": 0.8, "blue": 0.8}
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": new_sheet_id,
                        "startRowIndex": 0, "endRowIndex": 25,
                        "startColumnIndex": current_date.day, "endColumnIndex": current_date.day + 1
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": color}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
        current_date += timedelta(days=1)

    # 背景を黒で塗りつぶす
    requests.append(create_black_background_request(new_sheet_id, 25, 1000, 0, 32))
    return requests

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # Firebaseから学生データを取得
    student_indices = get_firebase_data('Students/student_info/student_index')
    print("取得した学生インデックス:", student_indices)  # デバッグ用
    if not student_indices or not isinstance(student_indices, dict):
        print("Firebaseから学生インデックスを取得できませんでした。")
        return

    for student_index, student_data in student_indices.items():
        student_name = student_data.get('student_name')
        if not student_name:
            print(f"学生インデックス {student_index} に学生名が見つかりませんでした。")
            continue

        # 学生のコースIDを取得
        course_ids = get_student_course_ids(student_index)
        if not course_ids:
            print(f"学生 {student_name} のコースIDが見つかりませんでした。")
            continue

        for course_id in course_ids:
            sheet_id = get_firebase_data(f'Courses/course_id/{course_id}/course_sheet_id')
            if not sheet_id:
                print(f"コースID {course_id} に関連付けられたシートIDが見つかりませんでした。")
                continue

            print(f"学生 {student_name} (ID: {student_index}): コース {course_id} に関連付けられたシートID {sheet_id} を取得しました。")

            # 月ごとの更新リクエストを準備して実行
            for month in range(1, 13):
                requests = prepare_update_requests(sheet_id, student_name, month, sheets_service, sheet_id)
                if not requests:
                    continue

                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={"requests": requests}
                ).execute()

if __name__ == "__main__":
    main()
