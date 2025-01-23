from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp
import httplib2
import time
import socket
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
    authorized_http = AuthorizedHttp(google_creds, http=httplib2.Http(timeout=60))
    return build('sheets', 'v4', cache_discovery=False, http=authorized_http)

# Firebaseからデータを取得
def get_firebase_data(ref_path):
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Firebaseデータ取得エラー: {e}")
        return None

# Firebaseからcourse_idリストを取得
def get_course_ids():
    courses = get_firebase_data("Courses/course_id")
    if not courses:
        print("コースデータが見つかりませんでした。")
        return []

    course_ids = []
    for i, course in enumerate(courses):
        if course and "course_name" in course:
            course_ids.append(i)  # インデックスがcourse_idに対応

    return course_ids

# Firebaseからstudent_indexリストを取得
def get_student_indices(class_index):
    student_indices_path = f"Class/class_index/{class_index}/student_index"
    student_indices_data = get_firebase_data(student_indices_path)

    if not student_indices_data:
        print(f"クラス {class_index} の学生インデックスが見つかりませんでした。")
        return []

    return student_indices_data.split(", ")  # カンマとスペースで分割

# Firebaseから学生名を取得
def get_student_names(student_indices):
    student_names = []

    for student_index in student_indices:
        student_name_path = f"Students/student_info/{student_index}/student_name"
        student_name = get_firebase_data(student_name_path)
        if student_name:
            student_names.append(student_name)

    return student_names

# セル更新リクエストを作成
def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": str(value)}}]}],
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

# 黒背景リクエストを作成
def create_black_background_request(sheet_id, start_row, end_row, start_col, end_col):
    black_color = {"red": 0.0, "green": 0.0, "blue": 0.0}
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                "startColumnIndex": start_col, "endColumnIndex": end_col
            },
            "cell": {"userEnteredFormat": {"backgroundColor": black_color}},
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
def prepare_update_requests(sheet_id, student_names, month, year=2025):
    requests = []

    # 列のサイズ設定
    requests.append(create_dimension_request(sheet_id, "COLUMNS", 0, 1, 35))
    requests.append(create_dimension_request(sheet_id, "COLUMNS", 1, 2, 100))

    # 学生名を設定
    requests.append(create_cell_update_request(sheet_id, 0, 0, "出席番号"))
    requests.append(create_cell_update_request(sheet_id, 0, 1, "学生名"))

    for i, name in enumerate(student_names):
        requests.append(create_cell_update_request(sheet_id, i + 1, 0, i + 1))
        requests.append(create_cell_update_request(sheet_id, i + 1, 1, name))

    # 日付の列設定
    current_date = datetime(year, month, 1)
    while current_date.month == month:
        col_index = (current_date.day + 1) * 2  # 適切な列インデックス計算
        requests.append(create_cell_update_request(sheet_id, 0, col_index, current_date.strftime("%m/%d")))
        current_date += timedelta(days=1)

    return requests

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # クラス情報を取得
    class_data = get_firebase_data("Class/class_index")
    if not class_data:
        print("クラス情報が見つかりませんでした。")
        return

    for class_index, class_info in class_data.items():
        student_indices = get_student_indices(class_index)
        if not student_indices:
            continue

        student_names = get_student_names(student_indices)
        if not student_names:
            continue

        course_ids = get_course_ids()
        for course_id in course_ids:
            sheet_id = get_sheet_id(course_id)
            if not sheet_id:
                continue

            for month in range(1, 13):
                print(f"Processing month {month} for class {class_index}, course {course_id}")
                requests = prepare_update_requests(sheet_id, student_names, month)
                if requests:
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=sheet_id,
                        body={"requests": requests}
                    ).execute()

if __name__ == "__main__":
    main()
