from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime

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
    if not isinstance(student_course_ids, list):
        student_course_ids = [student_course_ids]

    if not isinstance(courses, list) or len(courses) == 0:
        print("Courses data is invalid or empty.")
        return []

    class_names = []
    for course_id in student_course_ids:
        if course_id is None or not str(course_id).isdigit():
            continue

        course_id = int(course_id)
        if course_id < len(courses) and courses[course_id] is not None:
            course_data = courses[course_id]
            if 'class_name' in course_data:
                class_names.append(course_data['class_name'])
            else:
                print(f"Class name not found for course ID: {course_id}")
        else:
            print(f"Invalid course ID or course data: {course_id}")

    return class_names

def create_monthly_sheets(sheet_id, sheets_service):
    """スプレッドシートに1月～12月のシートを作成"""
    requests = []
    months = ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"]

    for month in months:
        requests.append({
            "addSheet": {
                "properties": {
                    "title": month
                }
            }
        })

    try:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": requests}
        ).execute()
        print("Monthly sheets created successfully.")
    except Exception as e:
        print(f"Error creating monthly sheets: {e}")

def update_monthly_sheets(sheet_id, sheets_service, class_names):
    """各月のシートにクラス名を追加"""
    months = ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"]

    for month in months:
        requests = []

        # 教科名を追加
        requests.append({
            "updateCells": {
                "start": {"sheetId": None, "rowIndex": 0, "columnIndex": 0},
                "rows": [{"values": [{"userEnteredValue": {"stringValue": "教科"}}]}],
                "fields": "userEnteredValue"
            }
        })

        # クラス名を追加
        for i, class_name in enumerate(class_names):
            requests.append({
                "updateCells": {
                    "start": {"sheetId": None, "rowIndex": i + 1, "columnIndex": 0},
                    "rows": [{"values": [{"userEnteredValue": {"stringValue": class_name}}]}],
                    "fields": "userEnteredValue"
                }
            })

        try:
            sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
            sheets = sheet_metadata.get('sheets', '')

            # シートIDを取得
            sheet_id_map = {sheet['properties']['title']: sheet['properties']['sheetId'] for sheet in sheets}
            if month in sheet_id_map:
                for request in requests:
                    request["updateCells"]["start"]["sheetId"] = sheet_id_map[month]

                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={"requests": requests}
                ).execute()
                print(f"{month} sheet updated successfully.")
            else:
                print(f"Sheet ID for {month} not found.")
        except Exception as e:
            print(f"Error updating {month} sheet: {e}")

def main():
    """メイン関数"""
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # Firebaseから必要なデータを取得
    sheet_id = get_firebase_data('Students/student_info/student_index/E534/sheet_id')
    student_course_ids = get_firebase_data('Students/enrollment/student_index/E534/course_id')
    courses = get_firebase_data('Courses/course_id')

    if not sheet_id:
        print("Sheet ID is missing or invalid.")
        return
    if student_course_ids is None:
        print("Student Course IDs data is missing or invalid.")
        return
    if courses is None:
        print("Courses data is missing or invalid.")
        return

    class_names = prepare_class_names(student_course_ids, courses)
    if not class_names:
        print("No valid class names found.")
        return

    create_monthly_sheets(sheet_id, sheets_service)
    update_monthly_sheets(sheet_id, sheets_service, class_names)

if __name__ == "__main__":
    main()
