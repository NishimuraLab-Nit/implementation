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

scope = ["https://spreadsheets.google.com/feeds", 
         "https://www.googleapis.com/auth/drive"]
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
# ユーティリティ
# ---------------------
def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    if not dt_str:
        print("Datetime string is empty.")
        return None
    try:
        return datetime.datetime.strptime(dt_str, fmt)
    except Exception as e:
        print(f"Error parsing datetime: {dt_str} with format {fmt}. Error: {e}")
        return None

def parse_hhmm_range(range_str):
    if not range_str:
        print("Time range string is empty.")
        return None, None
    try:
        start_str, end_str = range_str.split("~")
        sh, sm = map(int, start_str.split(":"))
        eh, em = map(int, end_str.split(":"))
        return datetime.time(sh, sm, 0), datetime.time(eh, em, 0)
    except Exception as e:
        print(f"Error parsing time range: {range_str}. Error: {e}")
        return None, None

def combine_date_and_time(date_dt, time_obj):
    combined = datetime.datetime(
        date_dt.year, date_dt.month, date_dt.day,
        time_obj.hour, time_obj.minute, time_obj.second
    )
    print(f"Combined date and time: {combined}")
    return combined

# ---------------------
# 出席判定ロジック
# ---------------------
def judge_attendance(entry_dt, exit_dt, start_dt, finish_dt):
    td_5min = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)
    print(f"Judging attendance: entry={entry_dt}, exit={exit_dt}, start={start_dt}, finish={finish_dt}")

    # (1) 欠席(×)
    if entry_dt >= finish_dt:
        print("Attendance result: ×")
        return "×", entry_dt, exit_dt, None

    # (2) 早退(△早)
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt < (finish_dt - td_5min)):
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        result = f"△早{delta_min}分"
        print(f"Attendance result: {result}")
        return result, entry_dt, exit_dt, None

    # (3) 遅刻(△遅)
    if (entry_dt > (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt <= (finish_dt + td_5min)):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        result = f"△遅{delta_min}分"
        print(f"Attendance result: {result}")
        return result, entry_dt, exit_dt, None

    # (4) 正常(○) ①
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt <= (finish_dt + td_5min)):
        print("Attendance result: ○")
        return "○", entry_dt, exit_dt, None

    # (4) 正常(○) ②: exit > finish+5分
    if (exit_dt is not None) and (exit_dt > (finish_dt + td_5min)):
        status_str = "○"
        original_exit = exit_dt
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt = original_exit
        print("Attendance result: ○ with extended exit")
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # (4) 正常(○) ③: exit=None
    if exit_dt is None:
        status_str = "○"
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt = None
        print("Attendance result: ○ with no exit")
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # その他
    print("Attendance result: ？")
    return "？", entry_dt, exit_dt, None

# ---------------------
# メイン処理
# ---------------------
def process_attendance_and_write_sheet():
    print("Starting attendance processing...")

    courses_data = get_data_from_firebase("Courses/course_id")
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    student_info_data = get_data_from_firebase("Students/student_info")

    if not courses_data or not attendance_data or not student_info_data:
        print("必要なデータが不足しています。")
        return

    for course_id, course_info in enumerate(courses_data):
        if not course_info or course_id == 0:
            print(f"Skipping invalid course_id: {course_id}")
            continue

        course_sheet_id = course_info.get("course_sheet_id")
        schedule = course_info.get("schedule", {})
        day = schedule.get("day")
        time_range = schedule.get("time")
        start_time, finish_time = parse_hhmm_range(time_range)

        if not course_sheet_id or not start_time or not finish_time:
            print(f"Invalid schedule or sheet ID for course_id: {course_id}")
            continue

        print(f"Processing course_id: {course_id}, sheet_id: {course_sheet_id}")

        try:
            sheet = gclient.open_by_key(course_sheet_id)
            print(f"Opened sheet with ID: {course_sheet_id}")
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"シートが見つかりません: {course_sheet_id}")
            continue

        sheet_name = datetime.datetime.now().strftime("%Y-%m")
        try:
            worksheet = sheet.worksheet(sheet_name)
            print(f"Found existing worksheet: {sheet_name}")
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet not found. Creating new worksheet: {sheet_name}")
            worksheet = sheet.add_worksheet(title=sheet_name, rows=100, cols=31)

        student_row_map = {}
        row_counter = 2  # Row 1 is header

        for student_id, entries in attendance_data.items():
            print(f"Processing student_id: {student_id}")

            student_index = student_info_data.get("student_id", {}).get(student_id, {}).get("student_index")
            if not student_index:
                print(f"Student index not found for student_id: {student_id}")
                continue

            enrollment_data = get_data_from_firebase(f"Students/enrollment/student_index/{student_index}")
            enrolled_courses = enrollment_data.get("course_id", "").split(",")
            if str(course_id) not in enrolled_courses:
                print(f"Student {student_id} not enrolled in course_id: {course_id}")
                continue

            if student_index not in student_row_map:
                student_row_map[student_index] = row_counter
                row_counter += 1

            for entry_key, entry_value in entries.items():
                if "entry" in entry_key:
                    exit_key = entry_key.replace("entry", "exit")
                    entry_dt = parse_datetime(entry_value.get("read_datetime"))
                    exit_dt = parse_datetime(entries.get(exit_key, {}).get("read_datetime"))

                    if not entry_dt:
                        print(f"No valid entry datetime for key: {entry_key}")
                        continue

                    entry_date = entry_dt.date()
                    if entry_date.strftime("%A") != day:
                        print(f"Entry date {entry_date} does not match course day {day}.")
                        continue

                    start_dt = combine_date_and_time(entry_date, start_time)
                    finish_dt = combine_date_and_time(entry_date, finish_time)

                    status, new_entry_dt, new_exit_dt, next_course_data = judge_attendance(entry_dt, exit_dt, start_dt, finish_dt)

                    col = entry_date.day + 1
                    row = student_row_map[student_index]
                    print(f"Preparing to write status {status} to sheet at row {row}, col {col}")

                    try:
                        current_value = worksheet.cell(row, col).value
                        if current_value != status:
                            worksheet.update_cell(row, col, status)
                            print(f"Successfully wrote status {status} to row {row}, col {col}")
                        else:
                            print(f"Skipped writing status {status} to row {row}, col {col} as it already contains the same value.")
                    except Exception as e:
                        print(f"Failed to write to sheet at row {row}, col {col}. Error: {e}")

    print("処理が完了しました。")

if __name__ == "__main__":
    process_attendance_and_write_sheet()
