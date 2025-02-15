import firebase_admin
from firebase_admin import credentials, db
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def initialize_firebase():
    if not firebase_admin._apps:
        # 資格情報のパスとデータベースURLを直接埋め込み
        cred = credentials.Certificate("/tmp/firebase_service_account.json")
        firebase_admin.initialize_app(
            cred,
            {"databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/"},
        )


def create_google_services():
    # Service Accountファイルとスコープも直接埋め込み
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


def fetch_students_data():
    """
    Firebaseから学生情報を取得して返します。
    学生情報が空の場合は ValueError を発生させます。
    """
    students_ref = db.reference("Students/student_info/student_index")
    students_data = students_ref.get()
    if not students_data:
        raise ValueError("No student data found in Firebase.")
    return students_data


def create_spreadsheet(sheets_service, student_number):
    spreadsheet_body = {
        "properties": {"title": f"{student_number}"},
    }

    spreadsheet = (
        sheets_service.spreadsheets()
        .create(body=spreadsheet_body, fields="spreadsheetId")
        .execute()
    )
    return spreadsheet.get("spreadsheetId")


def set_spreadsheet_permissions(drive_service, spreadsheet_id, student_email):
    # デフォルトの書き込み権限者のメールアドレスも直接埋め込み
    permissions = [
        {
            "type": "user",
            "role": "reader",
            "emailAddress": student_email,
        },
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


def save_spreadsheet_id_to_firebase(student_id, spreadsheet_id):
    students_ref = db.reference("Students/student_info/student_index")
    students_ref.child(student_id).update({"sheet_id": spreadsheet_id})


def create_spreadsheets_for_students():
    initialize_firebase()
    sheets_service, drive_service = create_google_services()
    students_data = fetch_students_data()

    for student_id, student_info in students_data.items():
        if not isinstance(student_info, dict):
            continue  # データが辞書形式でない場合はスキップ

        student_name = student_info.get("student_name")
        student_number = student_info.get("student_number")
        if not student_number:
            # 学生番号が無ければスキップ
            continue

        # 例: "abc123@denki.numazu-ct.ac.jp"
        student_email = f"{student_number}@denki.numazu-ct.ac.jp"

        try:
            # (1) スプレッドシートを作成
            spreadsheet_id = create_spreadsheet(sheets_service, student_number)

            # (2) アクセス権限を設定
            set_spreadsheet_permissions(drive_service, spreadsheet_id, student_email)

            # (3) FirebaseにスプレッドシートIDを保存
            save_spreadsheet_id_to_firebase(student_id, spreadsheet_id)

        except HttpError as e:
            print(
                f"Error creating spreadsheet for {student_name} ({student_number}): {e}"
            )


if __name__ == "__main__":
    create_spreadsheets_for_students()
