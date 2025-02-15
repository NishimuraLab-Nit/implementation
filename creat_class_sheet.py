import firebase_admin
from firebase_admin import credentials, db
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ===========================
# Firebaseアプリを初期化 (未初期化なら)
# ===========================
if not firebase_admin._apps:
    cred = credentials.Certificate("/tmp/firebase_service_account.json")
    firebase_admin.initialize_app(
        cred, 
        {"databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/"},
    )

# ===========================
# Googleサービスの初期化
# ===========================
# スコープとサービスアカウントファイルを直接ベタ書きしています
creds = service_account.Credentials.from_service_account_file(
    "/tmp/gcp_service_account.json", 
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
sheets_service = build("sheets", "v4", credentials=creds)
drive_service = build("drive", "v3", credentials=creds)


def create_spreadsheets_for_all_classes():
    try:
        # すべてのクラスデータを取得
        all_classes = db.reference("Class/class_index").get()
        print(f"[Debug] Type of all_classes: {type(all_classes)}")
        print(f"[Debug] Content of all_classes: {all_classes}")

        if not all_classes:
            print("[Debug] No classes found in the database.")
            return

        # 各クラスに対してスプレッドシートを作成
        for class_index, class_data in all_classes.items():
            # クラス担任のIDを取得
            class_teacher_id = class_data.get("class_teacher_id")
            if not class_teacher_id:
                print(f"[Debug] No class_teacher_id found for class index {class_index}")
                continue

            # クラス担任のメールアドレスを生成
            class_teacher_email = f"{class_teacher_id}@denki.numazu-ct.ac.jp"

            # 新しいスプレッドシートを作成
            spreadsheet_body = {
                "properties": {"title": f"{class_index}"},
            }
            spreadsheet = (
                sheets_service.spreadsheets()
                .create(body=spreadsheet_body, fields="spreadsheetId")
                .execute()
            )
            spreadsheet_id = spreadsheet.get("spreadsheetId")
            print(f"[Debug] Spreadsheet created for class {class_index}, ID: {spreadsheet_id}")

            # スプレッドシートのアクセス権限を設定
            permissions = [
                {"type": "user", "role": "writer", "emailAddress": class_teacher_email},
                {"type": "user", "role": "writer", "emailAddress": "naru.ibuki020301@gmail.com"},
            ]

            # パーミッションをバッチ処理で追加
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
            print(f"[Debug] Permissions set for spreadsheet ID: {spreadsheet_id}")

            # Firebase にスプレッドシートIDを保存
            class_ref = db.reference(f"Class/class_index/{class_index}/class_sheet_id")
            class_ref.set(spreadsheet_id)
            print(f"[Debug] Spreadsheet ID saved to Firebase for class index {class_index}")

    except HttpError as error:
        print(f"[Debug] API error occurred: {error}")
    except Exception as e:
        print(f"[Debug] Unexpected error: {e}")


# ===========================
# 実行
# ===========================
create_spreadsheets_for_all_classes()
