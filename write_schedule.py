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


# Firebaseからデータを一括で取得
def fetch_all_firebase_data():
    student_info = get_firebase_data('Students/student_info/student_index') or {}
    enrollments = get_firebase_data('Students/enrollment/student_index') or {}
    courses = get_firebase_data('Courses/course_id') or []
    return student_info, enrollments, courses


# Firebaseからデータを取得
def get_firebase_data(ref_path):
    return db.reference(ref_path).get()


# コースデータを辞書化する
def process_courses_data(courses):
    courses_dict = {}
    for idx, course in enumerate(courses):
        if course and isinstance(course, dict):
            courses_dict[str(idx)] = course
    return courses_dict


# ユニークなシート名を生成
def generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title):
    existing_titles = get_all_sheets(sheets_service, spreadsheet_id)
    if base_title not in existing_titles:
        return base_title

    index = 1
    while f"{base_title}-{index}" in existing_titles:
        index += 1
    return f"{base_title}-{index}"


# Google Sheetsのシートをすべて取得
def get_all_sheets(sheets_service, spreadsheet_id):
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet.get('sheets', [])
    return [sheet['properties']['title'] for sheet in sheets]


# シート更新リクエストを準備
def prepare_update_requests(sheet_id, course_names, month, sheets_service, spreadsheet_id, year=2025):
    if not course_names:
        print("コース名リストが空です。Firebaseから取得したデータを確認してください。")
        return []

    base_title = f"{year}-{str(month).zfill(2)}"
    sheet_title = generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title)
    add_sheet_request = {
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

    # 初期リクエスト
    requests = [add_sheet_request]

    # 日付やデータを設定するロジック
    # ...

    return requests


# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    student_info, enrollments, courses = fetch_all_firebase_data()
    courses_dict = process_courses_data(courses)

    for student_index, student_data in student_info.items():
        print(f"Processing student index: {student_index}")

        sheet_id = student_data.get('sheet_id')
        student_courses = enrollments.get(student_index, {}).get('course_id', [])

        course_names = [
            courses_dict.get(str(cid), {}).get('course_name')
            for cid in student_courses if str(cid) in courses_dict
        ]

        if not course_names:
            print(f"学生インデックス {student_index} のコース名が見つかりませんでした。")
            continue

        for month in range(1, 13):
            print(f"Processing month: {month} for student index: {student_index}")
            requests = prepare_update_requests(sheet_id, course_names, month, sheets_service, sheet_id)
            if not requests:
                print(f"月 {month} のシートを更新するリクエストがありません。")
                continue

            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={'requests': requests}
            ).execute()
            print(f"月 {month} のシートを正常に更新しました。")


if __name__ == "__main__":
    main()
