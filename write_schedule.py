from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta


# === Firebaseの初期化 ===
def initialize_firebase():
    try:
        firebase_cred = credentials.Certificate("firebase-adminsdk.json")
        initialize_app(firebase_cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })
        print("Firebase initialized successfully.")
    except Exception as e:
        print(f"Error initializing Firebase: {e}")


# === Google Sheets APIの初期化 ===
def get_google_sheets_service():
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
        return build('sheets', 'v4', credentials=google_creds)
    except Exception as e:
        print(f"Error initializing Google Sheets service: {e}")
        return None


# === Firebaseからデータを取得 ===
def get_firebase_data(ref_path):
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Error fetching data from Firebase for {ref_path}: {e}")
        return None


# === Google Sheets APIリクエスト作成関数 ===
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


def create_black_background_request(sheet_id, start_row, end_row, start_col, end_col):
    black_color = {"red": 0.0, "green": 0.0, "blue": 0.0}
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col
            },
            "cell": {"userEnteredFormat": {"backgroundColor": black_color}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    }


def create_sheet_request(sheet_title):
    return {
        "addSheet": {
            "properties": {
                "title": sheet_title,
                "gridProperties": {
                    "rowCount": 1000,
                    "columnCount": 32
                }
            }
        }
    }


# === Google Sheetsのシート関連処理 ===
def get_all_sheets(sheets_service, spreadsheet_id):
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        return [sheet['properties']['title'] for sheet in sheets]
    except Exception as e:
        print(f"Error fetching sheets: {e}")
        return []


def generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title):
    existing_titles = get_all_sheets(sheets_service, spreadsheet_id)
    if base_title not in existing_titles:
        return base_title

    counter = 1
    while f"{base_title} ({counter})" in existing_titles:
        counter += 1
    return f"{base_title} ({counter})"


# === 更新リクエストの準備 ===
def prepare_update_requests(sheet_id, course_names, month, sheets_service, spreadsheet_id, year=2025):
    if not course_names:
        print("Course names list is empty. Please check Firebase data.")
        return []

    # ユニークなシート名を生成
    base_title = f"{year}-{str(month).zfill(2)}"
    sheet_title = generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title)

    # シート作成リクエスト
    add_sheet_request = create_sheet_request(sheet_title)
    requests = [add_sheet_request]

    # 実際のbatchUpdateで新しいシートIDを取得
    try:
        response = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        ).execute()

        new_sheet_id = None
        for reply in response.get('replies', []):
            if 'addSheet' in reply:
                new_sheet_id = reply['addSheet']['properties']['sheetId']

        if new_sheet_id is None:
            print("Failed to retrieve new sheet ID.")
            return []

        # 更新リクエストを作成
        update_requests = [
            create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 100),
            create_dimension_request(new_sheet_id, "COLUMNS", 1, 32, 35),
            create_dimension_request(new_sheet_id, "ROWS", 0, 1, 120),
        ]

        # 教科名を追加
        update_requests.append(create_cell_update_request(new_sheet_id, 0, 0, "教科"))
        for i, name in enumerate(course_names):
            update_requests.append(create_cell_update_request(new_sheet_id, i + 1, 0, name))

        # 日付と背景色設定
        start_date = datetime(year, month, 1)
        end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

        japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
        current_date = start_date
        while current_date <= end_date:
            weekday = current_date.weekday()
            date_string = f"{current_date.strftime('%m')}/{current_date.strftime('%d')} {japanese_weekdays[weekday]}"
            update_requests.append(create_cell_update_request(new_sheet_id, 0, current_date.day, date_string))

            # 土日セルに色付け
            if weekday in (5, 6):
                color = {"red": 0.8, "green": 0.9, "blue": 1.0} if weekday == 5 else {"red": 1.0, "green": 0.8, "blue": 0.8}
                update_requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": new_sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 25,
                            "startColumnIndex": current_date.day,
                            "endColumnIndex": current_date.day + 1
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": color}},
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })

            current_date += timedelta(days=1)

        return update_requests
    except Exception as e:
        print(f"Error preparing update requests: {e}")
        return []


# === メイン処理 ===
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()
    if not sheets_service:
        return

    # Firebaseから学生データを取得
    student_indices = get_firebase_data('Students/student_info/student_index')
    if not student_indices or not isinstance(student_indices, dict):
        print("Failed to fetch student indices from Firebase.")
        return

    courses = get_firebase_data('Courses/course_id')
    if not courses or not isinstance(courses, list):
        print("Failed to fetch courses from Firebase.")
        return

    courses_dict = {str(index): course for index, course in enumerate(courses) if isinstance(course, dict)}

    for student_index, student_data in student_indices.items():
        print(f"Processing student index: {student_index}")

        sheet_id = student_data.get('sheet_id')
        student_course_ids = get_firebase_data(f'Students/enrollment/student_index/{student_index}/course_id')

        if not sheet_id or not isinstance(student_course_ids, list):
            print(f"Invalid data for student index {student_index}.")
            continue

        # 学生のコース名を取得
        course_names = [
            courses_dict[cid]['course_name']
            for cid in student_course_ids
            if cid in courses_dict and 'course_name' in courses_dict[cid]
        ]

        if not course_names:
            print(f"No course names found for student index {student_index}.")
            continue

        for month in range(1, 13):
            print(f"Updating sheet for month {month}")
            requests = prepare_update_requests(sheet_id, course_names, month, sheets_service, sheet_id)
            if not requests:
                print(f"No update requests for month {month}.")
                continue

            try:
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={'requests': requests}
                ).execute()
                print(f"Successfully updated sheet for month {month}.")
            except Exception as e:
                print(f"Failed to update sheet for month {month}: {e}")


if __name__ == "__main__":
    main()
