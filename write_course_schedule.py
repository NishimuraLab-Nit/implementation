from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp
import httplib2
import time
import socket
from datetime import datetime, timedelta

def initialize_firebase():
    firebase_cred = credentials.Certificate("firebase-adminsdk.json")
    initialize_app(firebase_cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

def get_google_sheets_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    authorized_http = AuthorizedHttp(google_creds, http=httplib2.Http(timeout=60))
    return build('sheets', 'v4', cache_discovery=False, http=authorized_http)

def get_firebase_data(ref_path):
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Firebaseデータ取得エラー: {e}")
        return None

def get_sheet_id(course_id):
    course_data = get_firebase_data(f'Courses/{course_id}')
    if course_data:
        return course_data.get("course_sheet_id")
    return None

def get_student_names(course_id):
    enrollment_data = get_firebase_data(f'Students/enrollment/course_id/{course_id}')
    if not enrollment_data:
        print(f"コース {course_id} の学生データが見つかりません。")
        return []

    student_indices = enrollment_data.get("student_index", "")
    indices = student_indices.split(",")

    student_names = []
    for index in indices:
        index = index.strip()
        student_info = get_firebase_data(f'Students/student_info/student_index/{index}')
        if student_info:
            student_names.append(student_info.get("student_name", ""))

    return student_names

def prepare_requests(sheet_id, student_names):
    requests = []

    # ユニークなシート名を生成
    base_title = f"出席表"
    sheet_title = base_title  # 必要に応じてユニーク化するロジックを追加

    # シート作成リクエスト
    add_sheet_request = {
        "addSheet": {
            "properties": {
                "title": sheet_title
            }
        }
    }
    requests.append(add_sheet_request)

    # 学生名をスプレッドシートに追加
    for i, name in enumerate(student_names):
        requests.append({
            "updateCells": {
                "rows": [
                    {
                        "values": [{"userEnteredValue": {"stringValue": name}}]
                    }
                ],
                "start": {"sheetId": sheet_id, "rowIndex": i + 1, "columnIndex": 0},
                "fields": "userEnteredValue"
            }
        })

    return requests

def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    course_ids = get_firebase_data('Courses/course_id')
    if not course_ids:
        print("コースIDが見つかりません。")
        return

    for course_id, course_data in enumerate(course_ids):
        if course_id == 0 or not course_data:
            continue

        sheet_id = get_sheet_id(course_id)
        if not sheet_id:
            print(f"コース {course_id} のシートIDが見つかりません。")
            continue

        student_names = get_student_names(course_id)
        if not student_names:
            print(f"コース {course_id} の学生名が見つかりません。")
            continue

        requests = prepare_requests(sheet_id, student_names)
        if not requests:
            print(f"コース {course_id} の更新リクエストが作成されませんでした。")
            continue

        try:
            execute_with_retry(
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={'requests': requests}
                )
            )
            print(f"コース {course_id} のシートを正常に更新しました。")
        except Exception as e:
            print(f"コース {course_id} の更新中にエラーが発生しました: {e}")

if __name__ == "__main__":
    main()
