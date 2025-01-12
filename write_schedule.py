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


# 条件付きフォーマットリクエストを作成
def create_conditional_formatting_request(sheet_id, start_row, end_row, start_col, end_col, color, formula):
    return {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                            "startColumnIndex": start_col, "endColumnIndex": end_col}],
                "booleanRule": {
                    "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": formula}]},
                    "format": {"backgroundColor": color}
                }
            },
            "index": 0
        }
    }


# 黒背景リクエストを作成
def create_black_background_request(sheet_id, start_row, end_row, start_col, end_col):
    black_color = {"red": 0.0, "green": 0.0, "blue": 0.0}
    return {
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                      "startColumnIndex": start_col, "endColumnIndex": end_col},
            "cell": {"userEnteredFormat": {"backgroundColor": black_color}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    }


# シート更新リクエストを準備
def prepare_update_requests(sheet_id, course_names):
    if not course_names:
        print("コース名リストが空です。Firebaseから取得したデータを確認してください。")
        return []

    requests = [
        {"appendDimension": {"sheetId": 0, "dimension": "COLUMNS", "length": 32}},
        create_dimension_request(0, "COLUMNS", 0, 1, 100),
        create_dimension_request(0, "COLUMNS", 1, 32, 35),
        create_dimension_request(0, "ROWS", 0, 1, 120),
        {"repeatCell": {"range": {"sheetId": 0},
                        "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                        "fields": "userEnteredFormat.horizontalAlignment"}},
        {"updateBorders": {"range": {"sheetId": 0, "startRowIndex": 0, "endRowIndex": 25, "startColumnIndex": 0,
                                     "endColumnIndex": 32},
                           "top": {"style": "SOLID", "width": 1},
                           "bottom": {"style": "SOLID", "width": 1},
                           "left": {"style": "SOLID", "width": 1},
                           "right": {"style": "SOLID", "width": 1}}},
        {"setBasicFilter": {"filter": {"range": {"sheetId": 0, "startRowIndex": 0, "endRowIndex": 25,
                                                 "startColumnIndex": 0, "endColumnIndex": 32}}}}
    ]

    # 教科名を設定
    requests.append(create_cell_update_request(0, 0, 0, "教科"))
    for i, name in enumerate(course_names):
        requests.append(create_cell_update_request(0, i + 1, 0, name))

    # 日付と条件付きフォーマット
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(2025, 1, 1)
    end_row = 25

    for i in range(31):
        date = start_date + timedelta(days=i)
        if date.month != 1:
            break
        weekday = date.weekday()
        date_string = f"{date.strftime('%m')}\n月\n{date.strftime('%d')}\n日\n⌢\n{japanese_weekdays[weekday]}\n⌣"
        requests.append(create_cell_update_request(0, 0, i + 1, date_string))

        # 土曜日と日曜日の条件付きフォーマット
        if weekday in (5, 6):
            color = {"red": 0.8, "green": 0.9, "blue": 1.0} if weekday == 5 else {"red": 1.0, "green": 0.8, "blue": 0.8}
            requests.append(create_conditional_formatting_request(
                0, 0, end_row, i + 1, i + 2, color,
                f'=ISNUMBER(SEARCH("{japanese_weekdays[weekday]}", INDIRECT(ADDRESS(1, COLUMN()))))'
            ))

    # 黒背景を設定
    requests.append(create_black_background_request(0, 25, 1000, 0, 1000))
    requests.append(create_black_background_request(0, 0, 1000, 32, 1000))

    return requests


# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # Firebaseからデータを取得
    sheet_id = get_firebase_data('Students/student_info/student_index/E534/sheet_id')
    student_course_ids = get_firebase_data('Students/enrollment/student_index/E534/course_id')
    courses = get_firebase_data('Courses/course_id')

    print("Sheet ID:", sheet_id)
    print("Student Course IDs:", student_course_ids)
    print("Courses:", courses)

    if not sheet_id or not isinstance(student_course_ids, dict) or not isinstance(courses, list):
        print("Firebaseから取得したデータが不正です。")
        return

    # コースIDとコース名をマッピング
    courses_dict = {str(i): course for i, course in enumerate(courses) if course}

    course_names = [
        courses_dict[cid]['course_name'] for cid in student_course_ids.values()
        if cid in courses_dict and 'course_name' in courses_dict[cid]
    ]

    # シート更新リクエストを準備
    requests = prepare_update_requests(sheet_id, course_names)
    if not requests:
        print("シートを更新するリクエストがありません。")
        return

    # シートを更新
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={'requests': requests}
    ).execute()
    print("シートを正常に更新しました。")


if __name__ == "__main__":
    main()
