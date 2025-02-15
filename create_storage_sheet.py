import firebase_admin
from firebase_admin import credentials, db
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def initialize_firebase():
    """
    Firebase Admin SDK の初期化を行います。
    すでに初期化されている場合は再初期化を行いません。
    """
    if not firebase_admin._apps:
        # 資格情報のパスとデータベースURLを直接埋め込み
        cred = credentials.Certificate("/tmp/firebase_service_account.json")
        firebase_admin.initialize_app(
            cred,
            {"databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/"},
        )


def create_google_services():
    """
    Google Sheets および Drive API のクライアントを作成して返します。
    """
    creds = service_account.Credentials.from_service_account_file(
        "/tmp/gcp_service_account.json",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    sheets_service = build("sheets", "v4", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)
    return sheets_service, drive_service


def set_spreadsheet_permissions(drive_service, spreadsheet_id):
    """
    指定のスプレッドシートに対して学生読み取り権限、既定ユーザ書き込み権限を設定します。
    """
    permissions = [
        {
            "type": "user",
            "role": "writer",
            "emailAddress": "naru.ibuki020301@gmail.com",
        },
    ]

    batch = drive_service.new_batch_http_request()
    for permission in permissions:
        batch.add(
            drive_service.permissions().create(
                fileId=spreadsheet_id,
                body=permission,
                fields="id",
            )
        )
    batch.execute()


def save_spreadsheet_id_to_firebase(spreadsheet_id):
    """
    Firebase Realtime Database の Students/attendance/attendance_sheet_id に
    スプレッドシートIDを保存します。
    """
    # Students/attendance ノードの参照を取得
    ref = db.reference("Students/attendance")
    # attendance_sheet_id キーでスプレッドシートIDを更新
    ref.update({"attendance_sheet_id": spreadsheet_id})
    print(f"Spreadsheet ID '{spreadsheet_id}' を Firebase に保存しました。")


def create_spreadsheet(sheets_service):
    """
    実際にスプレッドシートを作成する処理の例です。
    ※この関数の実装は環境や要件に合わせて適宜変更してください。
    """
    spreadsheet = {
        "properties": {
            "title": "Student Attendance Sheet"
        }
    }
    spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet, fields="spreadsheetId").execute()
    spreadsheet_id = spreadsheet.get("spreadsheetId")
    print(f"Spreadsheet created with ID: {spreadsheet_id}")
    return spreadsheet_id


def create_spreadsheets_for_students():
    """
    読み取り権限と書き込み権限の設定、FirebaseへのID登録を行います。
    """
    initialize_firebase()
    sheets_service, drive_service = create_google_services()

    try:
        # (1) スプレッドシートを作成
        spreadsheet_id = create_spreadsheet(sheets_service)

        # (2) アクセス権限を設定
        set_spreadsheet_permissions(drive_service, spreadsheet_id)

        # (3) FirebaseにスプレッドシートIDを保存
        save_spreadsheet_id_to_firebase(spreadsheet_id)

    except HttpError as e:
        print("Error creating spreadsheet:", e)


if __name__ == "__main__":
    create_spreadsheets_for_students()
