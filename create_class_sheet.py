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


def create_spreadsheets_for_courses():
    try:
        # Firebase データベースからコース情報を取得
        courses_ref = db.reference('Courses/course_id')
        all_courses = courses_ref.get()

        # デバッグ: データの型と内容を確認
        print("Debug: Type of all_courses:", type(all_courses))
        print("Debug: Content of all_courses:", all_courses)

        # データが取得できなかった場合のエラーハンドリング
        if not all_courses:
            raise ValueError("No course data found in Firebase.")

        # コースごとにスプレッドシートを作成
        for index, course in enumerate(all_courses):
            if course is None:  # 無効なデータをスキップ
                continue

            # コース名とクラス名を取得
            course_name = course.get('course_name', 'Unnamed Course')
            class_name = course.get('class_name', 'Unnamed Class')

            # スプレッドシートの名前を設定
            sheet_name = f"Courses/course_index/{class_name}"

            # 新しいスプレッドシートを作成
            spreadsheet = {
                'properties': {'title': sheet_name}
            }
            spreadsheet = sheets_service.spreadsheets().create(
                body=spreadsheet, fields='spreadsheetId'
            ).execute()
            spreadsheet_id = spreadsheet.get('spreadsheetId')
            print(f'Spreadsheet ID for {sheet_name}: {spreadsheet_id}')

            # スプレッドシートのアクセス権限を設定
            permissions = [
                {'type': 'user', 'role': 'reader', 'emailAddress': f'{class_name}@denki.numazu-ct.ac.jp'},
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

            # Firebase にスプレッドシート ID を保存
            course_ref = db.reference(f'Courses/course_index/{class_name}/course_sheet_id')
            course_ref.set(spreadsheet_id)

    except HttpError as error:
        print(f'API error occurred: {error}')
    except ValueError as e:
        print(e)


# 実行
create_spreadsheets_for_courses()
