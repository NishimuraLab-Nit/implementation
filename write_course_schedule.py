from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta

def initialize_firebase():
    # Initialize Firebase
    firebase_cred = credentials.Certificate("firebase-adminsdk.json")
    initialize_app(firebase_cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

def get_google_sheets_service():
    # Get Google Sheets API service
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    return build('sheets', 'v4', credentials=google_creds)

# Firebaseからデータを取得
def get_firebase_data(ref_path):
    return db.reference(ref_path).get()

# セル更新リクエストを作成
def create_cell_update_request(course_sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": course_sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

# 列や行のプロパティ設定リクエストを作成
def create_dimension_request(course_sheet_id, dimension, start_index, end_index, pixel_size):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": course_sheet_id, "dimension": dimension, "startIndex": start_index, "endIndex": end_index},
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize"
        }
    }

# 黒背景リクエストを作成
def create_black_background_request(course_sheet_id, start_row, end_row, start_col, end_col):
    black_color = {"red": 0.0, "green": 0.0, "blue": 0.0}
    return {
        "repeatCell": {
            "range": {"sheetId": course_sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                      "startColumnIndex": start_col, "endColumnIndex": end_col},
            "cell": {"userEnteredFormat": {"backgroundColor": black_color}},
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
# シート更新リクエストを準備
def prepare_update_requests(course_sheet_id, student_name, month, sheets_service, spreadsheet_id, year=2025):
    if not student_name:
        print("学生名が空です。Firebaseから取得したデータを確認してください。")
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

    # 以降のリクエストに新しいシートIDを使用
    requests = [
        {"appendDimension": {"sheetId": new_sheet_id, "dimension": "COLUMNS", "length": 32}},
        create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 100),
        create_dimension_request(new_sheet_id, "COLUMNS", 1, 32, 35),
        create_dimension_request(new_sheet_id, "ROWS", 0, 1, 120),
        {"repeatCell": {"range": {"sheetId": new_sheet_id},
                        "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                        "fields": "userEnteredFormat.horizontalAlignment"}},
        {"updateBorders": {"range": {"sheetId": new_sheet_id, "startRowIndex": 0, "endRowIndex": 25, "startColumnIndex": 0,
                                         "endColumnIndex": 32},
                           "top": {"style": "SOLID", "width": 1},
                           "bottom": {"style": "SOLID", "width": 1},
                           "left": {"style": "SOLID", "width": 1},
                           "right": {"style": "SOLID", "width": 1}}},
        {"setBasicFilter": {"filter": {"range": {"sheetId": new_sheet_id, "startRowIndex": 0, "endRowIndex": 25,
                                                     "startColumnIndex": 0, "endColumnIndex": 32}}}}
    ]

    # 教科名ではなく、学生名を設定
    requests.append(create_cell_update_request(new_sheet_id, 0, 0, "学生名"))
    requests.append(create_cell_update_request(new_sheet_id, 1, 0, student_name))
    # 日付と土日セルの色付け
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    end_row = 25

    current_date = start_date
    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = f"{current_date.strftime('%m')}\n月\n{current_date.strftime('%d')}\n日\n⌢\n{japanese_weekdays[weekday]}\n⌣"
        requests.append(create_cell_update_request(new_sheet_id, 0, current_date.day, date_string))

        # 土曜日と日曜日のセルに直接色を設定
        if weekday in (5, 6):
            color = {"red": 0.8, "green": 0.9, "blue": 1.0} if weekday == 5 else {"red": 1.0, "green": 0.8, "blue": 0.8}
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": new_sheet_id, 
                        "startRowIndex": 0, 
                        "endRowIndex": end_row, 
                        "startColumnIndex": current_date.day, 
                        "endColumnIndex": current_date.day + 1
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": color}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
        current_date += timedelta(days=1)

    # 残りのシートの背景色を黒に設定
    requests.append(create_black_background_request(new_sheet_id, 25, 1000, 0, 1000))
    requests.append(create_black_background_request(new_sheet_id, 0, 1000, 32, 1000))
    return requests

def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # Firebaseからデータを取得
    student_indices = get_firebase_data('Students/student_info/student_index')
    if not student_indices or not isinstance(student_indices, dict):
        print("Firebaseから学生インデックスを取得できませんでした。")
        return

    for student_index, student_data in student_indices.items():
        print(f"Processing student index: {student_index}")
        student_name = student_data.get('student_name')
        if not student_name:
            print(f"学生インデックス {student_index} に学生名が見つかりませんでした。")
            continue
    
        # 学生のコースIDを取得（カンマ区切りからリストに変換）
        student_course_ids = get_firebase_data(f'Students/enrollment/student_index/{student_index}/course_id')
        if not student_course_ids:
            print(f"学生インデックス {student_index} に関連付けられたコースIDが見つかりませんでした。")
            continue
    
        # コースIDが文字列の場合、リストに変換
        if isinstance(student_course_ids, str):
            student_course_ids = student_course_ids.split(', ')
    
        for course_id in student_course_ids:
            course_id = course_id.strip()  # 余分な空白を除去
            if not course_id:
                print(f"無効なコースIDが見つかりました: {course_id}")
                continue
    
            # 各コースIDからシートIDを取得
            course_sheet_id = get_firebase_data(f'Courses/course_id/{course_id}/course_sheet_id')
            if not course_sheet_id:
                print(f"コースID {course_id} に関連付けられたシートIDが見つかりませんでした。")
                continue
    
            for month in range(1, 13):
                print(f"Processing month: {month} for student index: {student_index} and course_id: {course_id}")
                requests = prepare_update_requests(course_sheet_id, student_name, month, sheets_service, course_sheet_id)
                if not requests:
                    print(f"月 {month} のシートを更新するリクエストがありません。")
                    continue
    
                # シートを更新
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=course_sheet_id,
                    body={'requests': requests}
                ).execute()
                print(f"月 {month} のシートを正常に更新しました。")


if __name__ == "__main__":
    main()
