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


def create_spreadsheets_for_students():
    """
    全学生のGoogleスプレッドシートを作成し、アクセス権限を設定。
    FirebaseデータベースにスプレッドシートIDを保存します。
    """
    try:
        # Firebase データベースから学生情報を取得
        students_ref = db.reference('Students/student_info/student_index')
        all_students = students_ref.get()

        if not all_students:
            raise ValueError("No student data found in Firebase.")

        for student_id, student_data in all_students.items():
            # 学生情報が辞書型であることを確認
            if not isinstance(student_data, dict):
                print(f"Skipping invalid student data for {student_id}")
                continue

            # 学生名と番号を取得
            student_name = student_data.get("student_name", "Unknown")
            student_number = student_data.get("student_number")
            if not student_number:
                print(f"Skipping student {student_name} due to missing student number.")
                continue

            # 新しいスプレッドシートを作成
            spreadsheet = {
                'properties': {'title': f"{student_name} ({student_number})"}
            }
            spreadsheet = sheets_service.spreadsheets().create(
                body=spreadsheet, fields='spreadsheetId'
            ).execute()
            spreadsheet_id = spreadsheet.get('spreadsheetId')
            print(f"Spreadsheet created for {student_name} (ID: {student_id}): {spreadsheet_id}")

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
            students_ref.child(student_id).update({'sheet_id': spreadsheet_id})

    except HttpError as error:
        print(f'Google API error: {error}')
    except ValueError as e:
        print(f"Value error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


# 実行
if __name__ == '__main__':
    create_spreadsheets_for_students()
