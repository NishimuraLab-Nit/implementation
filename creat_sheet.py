import firebase_admin
from firebase_admin import credentials, db
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Firebase アプリを初期化（未初期化の場合）
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Google Sheets と Drive API のスコープを定義
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = '/tmp/gcp_service_account.json'

# サービスアカウントファイルから資格情報を取得
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

# Google Sheets と Drive のサービスクライアントを作成
sheets_service = build('sheets', 'v4', credentials=creds)
drive_service = build('drive', 'v3', credentials=creds)


def create_spreadsheets_for_all_students():
    try:
        # Firebase データベースから全ての学生番号を取得
        students_ref = db.reference('Students/student_info/student_index/E534/student_number')
        all_students = students_ref.get()

        # データが取得できなかった場合のエラーハンドリング
        if not all_students:
            raise ValueError("No student data found in Firebase.")

        # all_students が辞書型かリスト型かに応じてループ処理を行う
        if isinstance(all_students, dict):
            student_numbers = all_students.keys()
        elif isinstance(all_students, list):
            student_numbers = all_students
        else:
            raise ValueError("Unexpected data format for student data.")

        for student_number in student_numbers:
            # 新しいスプレッドシートを作成
            spreadsheet = {
                'properties': {'title': str(student_number)}
            }
            spreadsheet = sheets_service.spreadsheets().create(
                body=spreadsheet, fields='spreadsheetId'
            ).execute()
            spreadsheet_id = spreadsheet.get('spreadsheetId')
            print(f'Spreadsheet ID for {student_number}: {spreadsheet_id}')

            # スプレッドシートのアクセス権限を設定
            permissions = [
                {'type': 'user', 'role': 'reader', 'emailAddress': f'{student_number}@denki.numazu-ct.ac.jp'},
                {'type': 'user', 'role': 'writer', 'emailAddress': 'naru.ibuki020301@gmail.com'}
            ]

            # パーミッションをバッチ処理で追加
            batch = drive_service.new_batch_http_request()
            for permission in permissions:
                batch.add(drive_service.permissions().create(
                    fileId=spreadsheet_id,
                    body=permission,
                    fields='id'
                ))
            batch.execute()

            # Firebase にスプレッドシートIDを保存
            student_ref = db.reference(f'Students/student_info/student_index/{student_number}')
            student_ref.update({'sheet_id': spreadsheet_id})

    except HttpError as error:
        print(f'API error occurred: {error}')
    except ValueError as e:
        print(e)


# 実行
create_spreadsheets_for_all_students()
