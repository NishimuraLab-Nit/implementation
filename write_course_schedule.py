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
    course_data = get_firebase_data(f'Courses/course_id/{course_id}')
    if course_data:
        return course_data.get('course_sheet_id', None)
    return None

def get_student_names(course_id):
    enrollment_data = get_firebase_data(f'Students/enrollment/course_id/{course_id}/student_index')
    if not enrollment_data:
        return []

    student_indices = [index.strip() for index in enrollment_data.split(',')]
    student_names = []

    for student_index in student_indices:
        student_info = get_firebase_data(f'Students/student_info/student_index/{student_index}')
        if student_info:
            student_name = student_info.get('student_name')
            if student_name:
                student_names.append(student_name)

    return student_names

def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    course_ids = get_firebase_data('Courses/course_id')
    if not course_ids:
        print("コースデータが見つかりませんでした。")
        return

    for course_id in range(1, len(course_ids)):
        sheet_id = get_sheet_id(course_id)
        if not sheet_id:
            print(f"コース {course_id} のシートIDが見つかりません。")
            continue

        student_names = get_student_names(course_id)
        if not student_names:
            print(f"コース {course_id} の学生データが見つかりません。")
            continue

        print(f"コース {course_id}: シートID = {sheet_id}, 学生数 = {len(student_names)}")

if __name__ == "__main__":
    main()
