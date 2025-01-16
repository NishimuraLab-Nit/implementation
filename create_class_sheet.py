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


def create_spreadsheets_for_class():
    try:
        # クラスのインデックスを取得（例として 'E5' を使用）
        class_indexes = ['E5']  # 必要に応じてリストを動的に生成

        for class_index in class_indexes:
            # クラス担任のメールアドレスを取得
            class_teacher_name = db.reference(f'Class/class_index/{class_index}/class_teacher_name').get()

            if not class_teacher_name:
                print(f"Class teacher name not found for class index {class_index}")
                continue

            # 新しいスプレッドシートを作成
            spreadsheet_body = {
                'properties': {
                    'title': f'{class_index} Class Spreadsheet'
                }
            }
            spreadsheet = sheets_service.spreadsheets().create(
                body=spreadsheet_body, fields='spreadsheetId'
            ).execute()
            spreadsheet_id = spreadsheet.get('spreadsheetId')
            print(f"Spreadsheet created with ID: {spreadsheet_id}")

            # スプレッドシートのアクセス権限を設定
            permissions = [
                {'type': 'user', 'role': 'writer', 'emailAddress': f'{class_teacher_name}@denki.numazu-ct.ac.jp'},
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
            print(f"Permissions set for spreadsheet ID: {spreadsheet_id}")

            # Firebase にスプレッドシート ID を保存
            class_ref = db.reference(f'Class/class_index/{class_index}/class_sheet_id')
            class_ref.set(spreadsheet_id)
            print(f"Spreadsheet ID saved to Firebase for class index {class_index}")

    except HttpError as error:
        print(f'API error occurred: {error}')


# 実行
create_spreadsheets_for_class()
