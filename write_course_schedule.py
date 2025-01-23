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
    student_data = get_firebase_data(f"Students/student_info/student_index/{student_index}")
    if not student_data:
        print(f"学生インデックス {student_index} のデータが見つかりません。")
        return []
    course_ids = student_data.get("course_id", "")
    return [course.strip() for course in course_ids.split(",") if course.strip()]

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # Firebaseから学生データを取得
    student_indices = get_firebase_data('Students/student_info/student_index')
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
