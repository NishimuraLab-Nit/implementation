import firebase_admin
from firebase_admin import credentials, db
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# 定数
FIREBASE_CREDENTIALS_PATH = '/tmp/firebase_service_account.json'
FIREBASE_DATABASE_URL = 'https://test-51ebc-default-rtdb.firebaseio.com/'
SERVICE_ACCOUNT_FILE = '/tmp/gcp_service_account.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
DEFAULT_WRITER_EMAIL = 'naru.ibuki020301@gmail.com'


def initialize_firebase():
    """Firebaseアプリを初期化します。"""
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred, {'databaseURL': FIREBASE_DATABASE_URL})


def create_google_services():
    """Google SheetsとDriveのサービスクライアントを作成します。"""
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    sheets_service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)
    return sheets_service, drive_service


def fetch_students_data():
    """Firebaseから学生情報を取得します。"""
    students_ref = db.reference('Students/student_info/student_index')
    students_data = students_ref.get()
    if not students_data:
        raise ValueError("No student data found in Firebase.")
    return students_data


def create_spreadsheet(sheets_service, student_number):
    """Google Sheetsに新しいスプレッドシートを作成します。"""
    spreadsheet_body = {'properties': {'title': f"{student_number}"}}
    spreadsheet = sheets_service.spreadsheets().create(
        body=spreadsheet_body, fields='spreadsheetId'
    ).execute()
    return spreadsheet.get('spreadsheetId')


def set_spreadsheet_permissions(drive_service, spreadsheet_id, student_email):
    """スプレッドシートのアクセス権限を設定します。"""
    permissions = [
        {'type': 'user', 'role': 'reader', 'emailAddress': student_email},
        {'type': 'user', 'role': 'writer', 'emailAddress': DEFAULT_WRITER_EMAIL},
    ]

    batch = drive_service.new_batch_http_request()
    for permission in permissions:
        batch.add(drive_service.permissions().create(
            fileId=spreadsheet_id, body=permission, fields='id'
        ))
    batch.execute()


def save_spreadsheet_id_to_firebase(student_id, spreadsheet_id):
    """FirebaseにスプレッドシートIDを保存します。"""
    students_ref = db.reference('Students/student_info/student_index')
    students_ref.child(student_id).update({'sheet_id': spreadsheet_id})


def create_spreadsheets_for_students():
    """すべての学生に対してスプレッドシートを作成し、設定します。"""
    initialize_firebase()
    sheets_service, drive_service = create_google_services()
    students_data = fetch_students_data()

    for student_id, student_info in students_data.items():
        if not isinstance(student_info, dict):
            continue  # 不正なデータをスキップ

        student_name = student_info.get("student_name")
        student_number = student_info.get("student_number")
        if not student_number:
            continue  # 学生番号がないデータをスキップ

        student_email = f'{student_number}@denki.numazu-ct.ac.jp'

        try:
            # スプレッドシートを作成
            spreadsheet_id = create_spreadsheet(sheets_service, student_number)
            
            # アクセス権限を設定
            set_spreadsheet_permissions(drive_service, spreadsheet_id, student_email)
            
            # FirebaseにスプレッドシートIDを保存
            save_spreadsheet_id_to_firebase(student_id, spreadsheet_id)
        except HttpError as e:
            print(f"Error creating spreadsheet for {student_name} ({student_number}): {e}")


if __name__ == '__main__':
    create_spreadsheets_for_students()
