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

def create_spreadsheet():
    try:
        # 学生番号を指定
        student_number = 'e19139'
        # Firebase データベースから学生情報を取得
        student_ref = db.reference(f'Students/enrollment/student_number/{student_number}')
        student_data = student_ref.get()

        # 学生データが見つからない場合はエラーを発生させる
        if student_data is None:
            raise ValueError("Student data not found in Firebase.")

        # コースIDを取得
        course_id = student_data['course_id'][0]

        # 新しいスプレッドシートを作成
        spreadsheet = {
            'properties': {'title': student_number}
        }
        spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
        sheet_id = spreadsheet.get('spreadsheetId')
        print(f'Spreadsheet ID: {sheet_id}')

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
        item_ref = db.reference(f'Students/item/student_number/{student_number}')
        item_ref.update({'sheet_id': sheet_id})

    except HttpError as error:
        print(f'API error occurred: {error}')
    except ValueError as e:
        print(e)

create_spreadsheet()
