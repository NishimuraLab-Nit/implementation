import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------
# Firebase & GSpread初期化
# ---------------------
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })
    print("Firebase initialized.")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gclient = gspread.authorize(creds)
print("Google Sheets API authorized.")

# ---------------------
# Firebaseアクセス関連
# ---------------------
def get_data_from_firebase(path):
    print(f"Fetching data from Firebase path: {path}")
    ref = db.reference(path)
    data = ref.get()
    if data is None:
        print(f"No data found at path: {path}")
    return data

# ---------------------
# ヘルパー関数
# ---------------------
def get_current_date():
    # 現在の日付を取得（例: "2025-01-26"）
    return datetime.datetime.now().strftime('%Y-%m-%d')

def get_current_sheet_name():
    # 現在の年月を取得して "YYYY-MM" 形式のシート名を生成（例: "2025-01"）
    return datetime.datetime.now().strftime('%Y-%m')

def map_date_to_column(sheet, current_date):
    """
    シートのヘッダー行から現在の日付に対応する列番号を取得します。
    ヘッダー行のフォーマットは "YYYY-MM-DD" と仮定しています。
    """
    try:
        # ヘッダー行を取得
        header_row = sheet.row_values(1)
        print(f"Header row: {header_row}")

        # 日付を検索
        if current_date in header_row:
            column = header_row.index(current_date) + 1  # 列番号は1から始まる
            print(f"Date '{current_date}' found at column {column}.")
            return column
        else:
            print(f"Date '{current_date}' not found in header row.")
            return None
    except Exception as e:
        print(f"Error mapping date to column: {e}")
        return None

def get_student_indices(student_indices_str):
    # "E523, E534" のような文字列をリストに変換
    return [s.strip() for s in student_indices_str.split(',')]

# ---------------------
# メイン処理
# ---------------------
def main():
    current_date = get_current_date()
    current_sheet_name = get_current_sheet_name()
    print(f"Current date: {current_date}")
    print(f"Current sheet name: {current_sheet_name}")

    # 1. Courses一覧を取得
    courses_data = get_data_from_firebase('Courses/course_id')
    if not courses_data:
        print("No courses found.")
        return

    # 2. 現在の曜日と一致するコースをフィルタリング
    matched_courses = []
    for idx, course_info in enumerate(courses_data):
        course_id = idx  # リストのインデックスが course_id
        if course_id == 0:
            continue  # インデックス0は無視
        if not course_info:
            print(f"Course data at index {course_id} is None.")
            continue
        schedule_day = course_info.get('schedule', {}).get('day')
        if schedule_day == datetime.datetime.now().strftime('%A'):
            matched_courses.append((course_id, course_info))
            print(f"Course {course_id} matches the current day.")
    
    if not matched_courses:
        print("No courses match the current day.")
        return

    # 3. 各マッチしたコースに対して処理
    for course_id, course_info in matched_courses:
        print(f"Processing Course ID: {course_id}, Course Name: {course_info.get('course_name')}")

        # 4. Enrollmentからstudent_indicesを取得
        enrollment_path = f"Students/enrollment/course_id/{course_id}/student_index"
        student_indices_str = get_data_from_firebase(enrollment_path)
        if not student_indices_str:
            print(f"No students enrolled in course {course_id}.")
            continue

        student_indices = get_student_indices(student_indices_str)
        print(f"Student indices for course {course_id}: {student_indices}")

        # 5. 各student_indexに対して処理
        for student_idx in student_indices:
            # 6. student_idを取得
            student_info_path = f"Students/student_info/student_index/{student_idx}/student_id"
            student_id = get_data_from_firebase(student_info_path)
            if not student_id:
                print(f"No student_id found for student_index {student_idx}.")
                continue
            print(f"Student Index: {student_idx}, Student ID: {student_id}")

            # 7. attendanceからdecisionを取得
            decision_path = f"Students/attendance/student_id/{student_id}/course_id/{course_id}/decision"
            decision = get_data_from_firebase(decision_path)
            if decision is None:
                print(f"No decision found for student_id {student_id} in course {course_id}.")
                continue
            print(f"Decision for student {student_id} in course {course_id}: {decision}")

            # 8. course_sheet_idを取得
            sheet_id = course_info.get('course_sheet_id')
            if not sheet_id:
                print(f"No course_sheet_id found for course {course_id}.")
                continue
            print(f"Course Sheet ID: {sheet_id}")

            try:
                # 9. Google Sheetを開く
                sh = gclient.open_by_key(sheet_id)
                print(f"Opened Google Sheet: {sh.title}")

                # シートの名前を一覧表示
                worksheets = sh.worksheets()
                print("Available worksheets:")
                for ws in worksheets:
                    print(f"- {ws.title}")

                # シート名を動的に指定（例: "2025-01"）
                try:
                    sheet = sh.worksheet(current_sheet_name)
                    print(f"Using worksheet: {sheet.title}")
                except gspread.exceptions.WorksheetNotFound:
                    print(f"Worksheet named '{current_sheet_name}' not found in spreadsheet {sheet_id}.")
                    continue

                # 列を決定（現在の日付に基づく列番号を取得）
                column = map_date_to_column(sheet, current_date)
                if column is None:
                    print(f"Cannot update decision for course {course_id}, student {student_idx} because date column is not found.")
                    continue

                # デバッグ用：シート内の全student_indicesを取得
                print("Fetching all student indices from the sheet for debugging...")
                all_values = sheet.col_values(1)  # 例としてA列をstudent_indexとして取得
                print(f"All student indices in the sheet (Column A): {all_values}")

                if not all_values:
                    print("No data found in Column A of the sheet.")
                    continue

                # 学生インデックスを検索（大文字・小文字を無視）
                student_row = None
                for row_num, cell_value in enumerate(all_values, start=1):
                    if cell_value.strip().upper() == student_idx.upper():
                        student_row = row_num
                        break

                if not student_row:
                    print(f"Student index {student_idx} not found in the sheet.")
                    continue
                print(f"Student index {student_idx} found at row {student_row}.")

                # セルにdecisionを入力
                sheet.update_cell(student_row, column, decision)
                print(f"Updated cell at row {student_row}, column {column} with decision '{decision}'.")

            except gspread.exceptions.SpreadsheetNotFound:
                print(f"Spreadsheet with ID {sheet_id} not found.")
            except gspread.exceptions.WorksheetNotFound:
                print(f"Worksheet '{current_sheet_name}' not found in spreadsheet {sheet_id}.")
            except Exception as e:
                print(f"Error updating Google Sheet for course {course_id}, student {student_idx}: {e}")

if __name__ == "__main__":
    main()
