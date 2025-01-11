from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta

def initialize_firebase():
    """Firebaseの初期化"""
    try:
        firebase_cred = credentials.Certificate("firebase-adminsdk.json")
        initialize_app(firebase_cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })
        print("Firebase initialized successfully.")
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        raise

def get_google_sheets_service():
    """Google Sheets APIのサービスを取得"""
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
        return build('sheets', 'v4', credentials=google_creds)
    except Exception as e:
        print(f"Error initializing Google Sheets API: {e}")
        raise

def get_firebase_data(ref_path):
    """Firebaseからデータを取得"""
    try:
        data = db.reference(ref_path).get()
        print(f"Data fetched from Firebase ({ref_path}): {data}")
        return data
    except Exception as e:
        print(f"Error fetching data from Firebase ({ref_path}): {e}")
        return None

def validate_firebase_data(sheet_id, student_course_ids, courses):
    """Firebaseデータの検証と整形"""
    if not isinstance(sheet_id, str) or not sheet_id.strip():
        raise ValueError("Invalid or missing sheet ID.")

    if isinstance(student_course_ids, int):
        student_course_ids = [str(student_course_ids)]
    elif isinstance(student_course_ids, str):
        student_course_ids = [student_course_ids]
    elif not isinstance(student_course_ids, list) or not student_course_ids:
        raise ValueError("Invalid or missing student course IDs.")

    if not isinstance(courses, list):
        raise ValueError("Courses data is not in list format.")

    valid_courses = {str(i): course for i, course in enumerate(courses) if course and 'class_name' in course}
    if not valid_courses:
        raise ValueError("Invalid or incomplete courses data.")

    return student_course_ids, valid_courses

def get_sheet_id_by_title(sheets_service, spreadsheet_id, sheet_title):
    """シートタイトルからシートIDを取得"""
    try:
        response = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in response.get('sheets', []):
            if sheet['properties']['title'] == sheet_title:
                return sheet['properties']['sheetId']
        raise ValueError(f"Sheet with title '{sheet_title}' not found.")
    except Exception as e:
        print(f"Error fetching sheet ID for title '{sheet_title}': {e}")
        raise

def prepare_update_requests(sheet_title, sheet_id, class_names, month_index):
    """Google Sheets更新用リクエストを準備"""
    if not class_names:
        print("Class names list is empty. Check Firebase data.")
        return []

    requests = []

    # 教科名を追加
    requests.append(create_cell_update_request(sheet_id, 0, 0, "教科"))
    requests.extend(create_cell_update_request(sheet_id, i + 1, 0, name) for i, name in enumerate(class_names))

    # 日付を追加
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(2025, month_index + 1, 1)

    for i in range(31):  # 最大31日分のデータ
        date = start_date + timedelta(days=i)
        if date.month != month_index + 1:  # 月が変わったら終了
            break
        weekday = date.weekday()
        date_string = f"{date.strftime('%m/%d')} ({japanese_weekdays[weekday]})"
        requests.append(create_cell_update_request(sheet_id, 0, i + 1, date_string))

    return requests

def main():
    """メイン関数"""
    try:
        initialize_firebase()
        sheets_service = get_google_sheets_service()

        # Firebaseから必要なデータを取得
        spreadsheet_id = get_firebase_data('Students/student_info/student_index/E534/sheet_id')
        student_course_ids = get_firebase_data('Students/enrollment/student_index/E534/course_id')
        courses = get_firebase_data('Courses/course_id')

        # データの検証と整形
        student_course_ids, courses_dict = validate_firebase_data(spreadsheet_id, student_course_ids, courses)

        # クラス名リストを作成
        class_names = [
            courses_dict[cid]['class_name'] for cid in student_course_ids
            if cid in courses_dict and 'class_name' in courses_dict[cid]
        ]

        if not class_names:
            print("Class names list is empty after processing. Check data integrity.")
            return

        # 1月～12月のシート作成
        prepare_monthly_sheets(spreadsheet_id, sheets_service)

        # 各月のデータを更新
        for month_index in range(12):  # 1月～12月
            sheet_title = f"{month_index + 1}月"
            try:
                sheet_id = get_sheet_id_by_title(sheets_service, spreadsheet_id, sheet_title)
            except ValueError as e:
                print(f"Skipping month {month_index + 1}: {e}")
                continue

            requests = prepare_update_requests(sheet_title, sheet_id, class_names, month_index)
            if not requests:
                print(f"No update requests for {month_index + 1}月.")
                continue

            try:
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': requests}
                ).execute()
                print(f"{month_index + 1}月のシートが正常に更新されました。")
            except Exception as e:
                print(f"Error updating sheet for {month_index + 1}月: {e}")

    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
