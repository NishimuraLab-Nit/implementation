import firebase_admin
from firebase_admin import credentials, db
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Firebase アプリを初期化（未初期化の場合）
if not firebase_admin._apps:
    # サービスアカウントの認証情報を指定
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    # Firebase アプリを初期化
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Google Sheets と Drive API のスコープを定義
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
# サービスアカウントファイルのパス
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
        students_ref = db.reference('Students/enrollment/student_index/E534/student_number')
        all_students = students_ref.get()

        if all_students is None:
            raise ValueError("No student data found in Firebase.")

        for student_number in all_students.keys():
            # 学生データを取得
            student_data = all_students[student_index]

            # 新しいスプレッドシートを作成
            spreadsheet = {
                'properties': {'title': student_number}
            }
            spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet, fields='sheet_id').execute()
            sheet_id = spreadsheet.get('sheet_id')
            print(f'Spreadsheet ID for {student_index}: {sheet_id}')

            # スプレッドシートのアクセス権限を設定
            permissions = [
                {'type': 'user', 'role': 'reader', 'emailAddress': f'{student_number}@denki.numazu-ct.ac.jp'},
                {'type': 'user', 'role': 'writer', 'emailAddress': 'naru.ibuki020301@gmail.com'}
            ]

            # パーミッションをバッチ処理で追加
            batch = drive_service.new_batch_http_request()
            for permission in permissions:
                batch.add(drive_service.permissions().create(
                    fileId=sheet_id,
                    body=permission,
                    fields='id'
                ))
            batch.execute()

            # Firebase にスプレッドシートIDを保存
            item_ref = db.reference(f'Students/student_info/student_index/{student_index}/sheet_id')
            item_ref.update({'sheet_id': sheet_id})

    except HttpError as error:
        print(f'API error occurred: {error}')
    except ValueError as e:
        print(e)

create_spreadsheets_for_all_students()
