import os
from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta


# ======== 初期化関数 ========
def initialize_firebase():
    """Firebaseの初期化"""
    try:
        firebase_cred = credentials.Certificate(os.getenv("FIREBASE_CRED_FILE", "firebase-adminsdk.json"))
        initialize_app(firebase_cred, {
            'databaseURL': os.getenv("FIREBASE_DB_URL", 'https://test-51ebc-default-rtdb.firebaseio.com/')
        })
        print("Firebase initialized.")
    except Exception as e:
        print(f"Failed to initialize Firebase: {e}")
        raise


def get_google_sheets_service():
    """Google Sheets APIサービスを取得"""
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        google_creds = Credentials.from_service_account_file(
            os.getenv("GOOGLE_CRED_FILE", "google-credentials.json"), scopes=scopes
        )
        return build('sheets', 'v4', credentials=google_creds)
    except Exception as e:
        print(f"Failed to initialize Google Sheets API service: {e}")
        raise


# ======== Firebaseデータ取得関数 ========
def get_firebase_data(ref_path):
    """Firebaseから指定パスのデータを取得"""
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Error fetching data from Firebase ({ref_path}): {e}")
        return None


# ======== データ正規化関数 ========
def normalize_course_ids(course_ids):
    """
    Firebaseから取得した`course_id`をリスト形式に変換。
    - 辞書形式の場合は値をリストに変換。
    """
    if isinstance(course_ids, dict):
        return list(course_ids.values())
    elif isinstance(course_ids, list):
        return course_ids
    else:
        return None


# ======== Google Sheets用リクエスト作成関数 ========
def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }


def create_dimension_request(sheet_id, dimension, start_index, end_index, pixel_size):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": dimension, "startIndex": start_index, "endIndex": end_index},
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize"
        }
    }


def prepare_update_requests(sheet_id, class_names):
    """Google Sheets用のバッチリクエストを準備"""
    if not class_names:
        print("Class names list is empty. Check data retrieved from Firebase.")
        return []

    # 基本設定リクエスト
    requests = [
        {"appendDimension": {"sheetId": 0, "dimension": "COLUMNS", "length": 32}},
        create_dimension_request(0, "COLUMNS", 0, 1, 100),
        create_dimension_request(0, "COLUMNS", 1, 32, 35),
        create_dimension_request(0, "ROWS", 0, 1, 120),
        {"repeatCell": {"range": {"sheetId": 0},
                        "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                        "fields": "userEnteredFormat.horizontalAlignment"}}
    ]

    # クラス名をセルに追加
    requests.append(create_cell_update_request(0, 0, 0, "教科"))
    requests.extend(create_cell_update_request(0, i + 1, 0, name) for i, name in enumerate(class_names))

    # 日付列を追加
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(2025, 1, 1)

    for i in range(31):
        date = start_date + timedelta(days=i)
        weekday = date.weekday()
        date_string = f"{date.strftime('%m')}月\n{date.strftime('%d')}日\n{japanese_weekdays[weekday]}"
        requests.append(create_cell_update_request(0, 0, i + 1, date_string))

    return requests


# ======== メイン処理 ========
def main():
    try:
        # FirebaseとGoogle Sheetsの初期化
        initialize_firebase()
        sheets_service = get_google_sheets_service()

        # Firebaseから必要なデータを取得
        student_index = "E534"  # 必要に応じて動的に指定
        sheet_id = get_firebase_data(f'Students/student_info/student_index/{student_index}/sheet_id')
        if not sheet_id:
            print(f"Error: Sheet ID not found for student_index '{student_index}'")
            return

        raw_course_ids = get_firebase_data(f'Students/enrollment/student_index/{student_index}/course_id')
        student_course_ids = normalize_course_ids(raw_course_ids)
        if not student_course_ids:
            print(f"Error: Invalid course ID data for student_index '{student_index}'")
            return

        courses = get_firebase_data('Courses/course_id')
        if not isinstance(courses, list):
            print("Error: Invalid courses data from Firebase")
            return

        # クラス名を抽出
        courses_dict = {i: course for i, course in enumerate(courses) if course}
        class_names = [
            courses_dict[cid]['course_name'] for cid in student_course_ids
            if cid in courses_dict and 'class_name' in courses_dict[cid]
        ]

        # Google Sheetsの更新リクエストを準備
        requests = prepare_update_requests(sheet_id, class_names)
        if not requests:
            print("No requests to update the sheet.")
            return

        # Google Sheets APIで更新
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={'requests': requests}
        ).execute()
        print("Sheet updated successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
