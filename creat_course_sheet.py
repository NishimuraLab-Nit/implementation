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

def create_spreadsheets_for_courses():
    try:
        # Firebase データベースから全てのコースを取得
        courses_ref = db.reference('Courses/course_id')
        all_courses = courses_ref.get()

        if all_courses is None:
            raise ValueError("No course data found in Firebase.")

        for course_id, course_data in enumerate(all_courses):
            if course_data is None:
                continue

            class_name = course_data.get('class_name', f"Course {course_id}")

            # 新しいスプレッドシートを作成
            spreadsheet = {
                'properties': {'title': class_name}
            }
            spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
            sheet_id = spreadsheet.get('spreadsheetId')
            print(f'Spreadsheet ID for {class_name}: {sheet_id}')

            # スプレッドシートのアクセス権限を設定
            permissions = [
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
            item_ref = db.reference(f'Students/course_id/{course_id}')
            item_ref.update({'course_sheet_id': sheet_id})

    except HttpError as error:
        print(f'API error occurred: {error}')
    except ValueError as e:
        print(e)

create_spreadsheets_for_courses()
