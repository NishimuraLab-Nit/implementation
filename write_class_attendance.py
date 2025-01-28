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
def get_current_date_details():
    now = datetime.datetime.now()
    current_day = now.strftime('%A')          # 曜日 (例: "Sunday")
    current_sheet_name = now.strftime('%Y-%m') # シート名 (例: "2025-01")
    current_day_of_month = now.day            # 日付 (数値, 例: 26)
    return current_day, current_sheet_name, current_day_of_month, now

def map_date_and_period_to_column(day_of_month, period):
    # column = ((day_as_number) * 4) + period - 2
    return (day_of_month * 4) + period - 2

def get_student_indices(student_indices_str):
    # "E523, E534" のような文字列をリストに変換
    return [s.strip() for s in student_indices_str.split(',')]

def is_within_period(current_datetime, period_time_str):
    # period_time_strは "13:10~14:40" の形式
    try:
        start_str, end_str = period_time_str.split('~')
        start_time = datetime.datetime.strptime(start_str.strip(), '%H:%M').time()
        end_time = datetime.datetime.strptime(end_str.strip(), '%H:%M').time()
        return start_time <= current_datetime.time() <= end_time
    except ValueError:
        print(f"Invalid period format: {period_time_str}")
        return False

# ---------------------
# メイン処理
# ---------------------
def main():
    current_day, current_sheet_name, current_day_of_month, now = get_current_date_details()
    print(f"Current day: {current_day}")
    print(f"Current sheet name: {current_sheet_name}")
    print(f"Current day of month: {current_day_of_month}")
    print(f"Current datetime: {now}")

    # 1. クラス一覧を取得（必要に応じてクラス_indexを指定）
    class_index = 'E5'  # 例として 'E5' を使用。必要に応じて変更。
    class_path = f"Class/class_index/{class_index}"
    class_data = get_data_from_firebase(class_path)
    if not class_data:
        print(f"No data found for class_index {class_index}.")
        return

    student_indices_str = class_data.get('student_index')
    if not student_indices_str:
        print(f"No student_index found for class_index {class_index}.")
        return

    student_indices = get_student_indices(student_indices_str)
    print(f"Student indices for class {class_index}: {student_indices}")

    # 2. 各student_indexに対して処理
    for idx, student_idx in enumerate(student_indices, start=1):
        row_number = idx + 2  # 各student_idxのインデックスに2を加える
        print(f"\nProcessing Student {student_idx} (List Index: {idx}, Sheet Row: {row_number})")

        # 3. student_idを取得
        student_id_path = f"Students/student_info/student_index/{student_idx}/student_id"
        student_id = get_data_from_firebase(student_id_path)
        if not student_id:
            print(f"No student_id found for student_index {student_idx}.")
            continue
        print(f"Student ID: {student_id}")

        # 4. attendanceからentryとexitを取得
        attendance_path = f"Students/attendance/student_id/{student_id}"
        attendance_data = get_data_from_firebase(attendance_path)
        if not attendance_data:
            print(f"No attendance data found for student_id {student_id}.")
            continue

        # entryとexitの取得（複数ある場合は最新のものを使用）
        entries = [v['read_datetime'] for k, v in attendance_data.items() if k.startswith('entry') and 'read_datetime' in v]
        exits = [v['read_datetime'] for k, v in attendance_data.items() if k.startswith('exit') and 'read_datetime' in v]

        entry = max(entries) if entries else None
        exit_ = max(exits) if exits else None

        if not entry:
            print(f"No entry found for student_id {student_id}. Skipping.")
            continue

        # 5. 現在のdatetime
        current_datetime = now

        # 6. entryが存在するがexitがない場合
        if not exit_:
            status = "⚪︎"
            print(f"Entry exists but exit is missing for student_id {student_id}. Setting status to {status}.")

            # 各periodの時間内かどうかをチェック
            course_ids_str = class_data.get('course_id', '')
            course_ids = [cid.strip() for cid in course_ids_str.split(',') if cid.strip()]
            period_found = False

            for course_id in course_ids:
                course_schedule_path = f"Courses/course_id/{course_id}/schedule"
                course_schedule = get_data_from_firebase(course_schedule_path)
                if not course_schedule:
                    print(f"No schedule found for course_id {course_id}.")
                    continue

                period_time_str = course_schedule.get('time')
                period = course_schedule.get('period')
                if not period_time_str or not period:
                    print(f"Incomplete schedule data for course_id {course_id}.")
                    continue

                if is_within_period(current_datetime, period_time_str):
                    print(f"Current time is within period {period} for course_id {course_id}.")

                    # columnの計算
                    column = map_date_and_period_to_column(current_day_of_month, period)
                    print(f"Calculated column: {column}")

                    # Google Sheetsの更新
                    course_sheet_id = course_schedule.get('course_sheet_id')
                    if not course_sheet_id:
                        print(f"No course_sheet_id found for course_id {course_id}.")
                        continue

                    try:
                        sh = gclient.open_by_key(course_sheet_id)
                        print(f"Opened Google Sheet: {sh.title}")

                        sheet = sh.worksheet(current_sheet_name)
                        print(f"Using worksheet: {sheet.title}")

                        sheet.update_cell(row_number, column, status)
                        print(f"Updated cell at row {row_number}, column {column} with status '{status}'.")
                        period_found = True
                        break  # 一致するperiodが見つかったらループを抜ける
                    except gspread.exceptions.SpreadsheetNotFound:
                        print(f"Spreadsheet with ID {course_sheet_id} not found.")
                    except gspread.exceptions.WorksheetNotFound:
                        print(f"Worksheet named '{current_sheet_name}' not found in spreadsheet {course_sheet_id}.")
                    except Exception as e:
                        print(f"Error updating Google Sheet for course {course_id}, student {student_idx}: {e}")

            if not period_found:
                print(f"No matching period found for current datetime {current_datetime} for student_id {student_id}.")

        else:
            # 7. entryとexitの両方が存在する場合
            course_ids_str = class_data.get('course_id', '')
            course_ids = [cid.strip() for cid in course_ids_str.split(',') if cid.strip()]

            for course_id in course_ids:
                decision_path = f"Students/attendance/student_id/{student_id}/course_id/{course_id}/decision"
                period_path = f"Students/attendance/student_id/{student_id}/course_id/{course_id}/period"

                decision = get_data_from_firebase(decision_path)
                period = get_data_from_firebase(period_path)

                if decision is None or period is None:
                    print(f"Missing decision or period for course_id {course_id}, student_id {student_id}.")
                    continue

                print(f"Course ID {course_id}: Decision='{decision}', Period={period}")

                # columnの計算
                column = map_date_and_period_to_column(current_day_of_month, period)
                print(f"Calculated column: {column}")

                # Google Sheetsの更新
                course_sheet_id = get_data_from_firebase(f"Courses/course_id/{course_id}/course_sheet_id")
                if not course_sheet_id:
                    print(f"No course_sheet_id found for course_id {course_id}.")
                    continue

                try:
                    sh = gclient.open_by_key(course_sheet_id)
                    print(f"Opened Google Sheet: {sh.title}")

                    sheet = sh.worksheet(current_sheet_name)
                    print(f"Using worksheet: {sheet.title}")

                    sheet.update_cell(row_number, column, decision)
                    print(f"Updated cell at row {row_number}, column {column} with decision '{decision}'.")
                except gspread.exceptions.SpreadsheetNotFound:
                    print(f"Spreadsheet with ID {course_sheet_id} not found.")
                except gspread.exceptions.WorksheetNotFound:
                    print(f"Worksheet named '{current_sheet_name}' not found in spreadsheet {course_sheet_id}.")
                except Exception as e:
                    print(f"Error updating Google Sheet for course {course_id}, student {student_idx}: {e}")

if __name__ == "__main__":
    main()
