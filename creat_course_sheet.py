import firebase_admin
from firebase_admin import credentials, db
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# -------------------------
# Firebaseアプリ初期化
# -------------------------
try:
    firebase_admin.get_app()
except ValueError:
    cred = credentials.Certificate("/tmp/firebase_service_account.json")
    firebase_admin.initialize_app(
        cred,
        {"databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/"},
    )


# -------------------------
# Google認証情報の作成
# -------------------------
google_creds = service_account.Credentials.from_service_account_file(
    "/tmp/gcp_service_account.json",
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)


def create_spreadsheets_for_courses():
    """
    Courses配下のコースごとに新規スプレッドシートを作成し、
    権限設定とFirebase上へのシートID保存を行います。
    """
    try:
        # すべてのコースデータを取得
        courses_ref = db.reference("Courses/course_id")
        all_courses = courses_ref.get()

        if not all_courses:
            print("[Debug] No course data found in Firebase.")
            return

        # コースごとに新規スプレッドシートを作成
        for course_index, course_data in enumerate(all_courses):
            if not course_data:
                continue

            course_name = course_data.get("course_name", "Unnamed Course")

            # -------------------------
            # 新規スプレッドシート作成
            # -------------------------
            with build("sheets", "v4", credentials=google_creds) as sheets_service:
                spreadsheet_body = {
                    "properties": {
                        "title": course_name,
                    }
                }
                spreadsheet = (
                    sheets_service.spreadsheets()
                    .create(body=spreadsheet_body, fields="spreadsheetId")
                    .execute()
                )
                sheet_id = spreadsheet.get("spreadsheetId")
                print(f"[Debug] Spreadsheet created for '{course_name}' ID: {sheet_id}")

            # -------------------------
            # 権限設定（Drive API）
            # -------------------------
            with build("drive", "v3", credentials=google_creds) as drive_service:
                permissions = [
                    {
                        "type": "user",
                        "role": "writer",
                        "emailAddress": "naru.ibuki020301@gmail.com",
                    }
                ]
                batch = drive_service.new_batch_http_request()

                for permission in permissions:
                    batch.add(
                        drive_service.permissions().create(
                            fileId=sheet_id,
                            body=permission,
                            fields="id",
                        )
                    )
                batch.execute()
                print(f"[Debug] Permissions set for spreadsheet ID: {sheet_id}")

            # -------------------------
            # FirebaseにシートIDを保存
            # -------------------------
            course_ref = db.reference(f"Courses/course_id/{course_index}")
            course_ref.update({"course_sheet_id": sheet_id})
            print(f"[Debug] Spreadsheet ID saved to Firebase for course index={course_index}")

    except HttpError as error:
        print(f"[Debug] API error occurred: {error}")
    except Exception as e:
        print(f"[Debug] An error occurred: {e}")


# -------------------------
# メイン処理
# -------------------------
create_spreadsheets_for_courses()
