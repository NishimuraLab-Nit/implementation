from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta

def initialize_firebase():
    """Firebaseの初期化"""
    firebase_cred = credentials.Certificate("firebase-adminsdk.json")
    initialize_app(firebase_cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

def get_google_sheets_service():
    """Google Sheets APIのサービスを取得"""
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    return build('sheets', 'v4', credentials=google_creds)

def get_firebase_data(ref_path):
    """Firebaseからデータを取得"""
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Error retrieving data from Firebase: {e}")
        return None

def prepare_class_names(student_course_ids, courses):
    """クラス名を準備"""
    # Student Course IDsをリスト形式に変換
    if not isinstance(student_course_ids, list):
        student_course_ids = [student_course_ids]

    # Coursesデータの検証
    if not isinstance(courses, list) or len(courses) == 0:
        print("Courses data is invalid or empty.")
        return []

    # クラス名を抽出
    class_names = []
    for course_id in student_course_ids:
        # コースIDが無効の場合をスキップ
        if course_id is None or not str(course_id).isdigit():
            continue

        course_id = int(course_id)  # 数値として扱う
        if course_id < len(courses) and courses[course_id] is not None:
            course_data = courses[course_id]
            if 'class_name' in course_data:
                class_names.append(course_data['class_name'])
            else:
                print(f"Class name not found for course ID: {course_id}")
        else:
            print(f"Invalid course ID or course data: {course_id}")

    return class_names

def prepare_update_requests(sheet_id, class_names):
    """Google Sheets更新用リクエストを準備"""
    if not class_names:
        print("Class names list is empty. Check data retrieved from Firebase.")
        return []

    requests = [
        {"appendDimension": {"sheetId": 0, "dimension": "COLUMNS", "length": 32}},
        {"updateDimensionProperties": {
            "range": {"sheetId": 0, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 100},
            "fields": "pixelSize"
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": 0, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 120},
            "fields": "pixelSize"
        }},
        {"repeatCell": {
            "range": {"sheetId": 0},
            "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
            "fields": "userEnteredFormat.horizontalAlignment"
        }},
    ]

    # 教科名を追加
    requests.append({
        "updateCells": {
            "start": {"sheetId": 0, "rowIndex": 0, "columnIndex": 0},
            "rows": [{"values": [{"userEnteredValue": {"stringValue": "教科"}}]}],
            "fields": "userEnteredValue"
        }
    })

    # クラス名を追加
    for i, class_name in enumerate(class_names):
        requests.append({
            "updateCells": {
                "start": {"sheetId": 0, "rowIndex": i + 1, "columnIndex": 0},
                "rows": [{"values": [{"userEnteredValue": {"stringValue": class_name}}]}],
                "fields": "userEnteredValue"
            }
        })

    return requests

def main():
    """メイン関数"""
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # Firebaseから必要なデータを取得
    sheet_id = get_firebase_data('Students/student_info/student_index/E534/sheet_id')
    student_course_ids = get_firebase_data('Students/enrollment/student_index/E534/course_id')
    courses = get_firebase_data('Courses/course_id')

    # データの検証
    if not sheet_id:
        print("Sheet ID is missing or invalid.")
        return
    if student_course_ids is None:
        print("Student Course IDs data is missing or invalid.")
        return
    if courses is None:
        print("Courses data is missing or invalid.")
        return

    # クラス名の準備
    class_names = prepare_class_names(student_course_ids, courses)
    if not class_names:
        print("No valid class names found.")
        return

    # リクエストを準備
    requests = prepare_update_requests(sheet_id, class_names)
    if not requests:
        print("No requests to update the sheet.")
        return

    # シートの更新を実行
    try:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": requests}
        ).execute()
        print("Sheet updated successfully.")
    except Exception as e:
        print(f"Error updating the sheet: {e}")

if __name__ == "__main__":
    main()
