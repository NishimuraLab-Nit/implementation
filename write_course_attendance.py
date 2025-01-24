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

scope = ["https://spreadsheets.google.com/feeds", 
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gclient = gspread.authorize(creds)

# ---------------------
# Firebaseアクセス関連
# ---------------------
def get_data_from_firebase(path):
    ref = db.reference(path)
    return ref.get()

# ---------------------
# ユーティリティ
# ---------------------
def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    if not dt_str:
        return None
    try:
        return datetime.datetime.strptime(dt_str, fmt)
    except:
        return None

def parse_hhmm_range(range_str):
    if not range_str:
        return None, None
    try:
        start_str, end_str = range_str.split("~")
        sh, sm = map(int, start_str.split(":"))
        eh, em = map(int, end_str.split(":"))
        return datetime.time(sh, sm, 0), datetime.time(eh, em, 0)
    except:
        return None, None

def combine_date_and_time(date_dt, time_obj):
    return datetime.datetime(
        date_dt.year, date_dt.month, date_dt.day,
        time_obj.hour, time_obj.minute, time_obj.second
    )

# ---------------------
# 出席判定ロジック
# ---------------------
def judge_attendance(entry_dt, exit_dt, start_dt, finish_dt):
    td_5min = datetime.timedelta(minutes=5)

    if entry_dt >= finish_dt:
        return "×"

    if (entry_dt <= (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt < (finish_dt - td_5min)):
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        return f"△早{delta_min}分"

    if (entry_dt > (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt <= (finish_dt + td_5min)):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分"

    if (entry_dt <= (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt <= (finish_dt + td_5min)):
        return "○"

    return "？"

# ---------------------
# メイン処理
# ---------------------
def process_attendance_and_write_sheet():
    courses_data = get_data_from_firebase("Courses/course_id")
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    student_info_data = get_data_from_firebase("Students/student_info")

    if not courses_data or not attendance_data or not student_info_data:
        print("必要なデータが不足しています。")
        return

    for course_id, course_info in enumerate(courses_data):
        if not course_info or course_id == 0:
            continue

        course_sheet_id = course_info.get("course_sheet_id")
        schedule = course_info.get("schedule", {})
        day = schedule.get("day")
        time_range = schedule.get("time")
        start_time, finish_time = parse_hhmm_range(time_range)

        if not course_sheet_id or not start_time or not finish_time:
            continue

        try:
            sheet = gclient.open_by_key(course_sheet_id)
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"シートが見つかりません: {course_sheet_id}")
            continue

        sheet_name = datetime.datetime.now().strftime("%Y-%m")
        try:
            worksheet = sheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=sheet_name, rows=100, cols=31)

        for student_id, entries in attendance_data.items():
            student_index = student_info_data.get("student_id", {}).get(student_id, {}).get("student_index")
            if not student_index:
                continue

            enrollment_data = get_data_from_firebase(f"Students/enrollment/student_index/{student_index}")
            enrolled_courses = enrollment_data.get("course_id", "").split(",")
            if str(course_id) not in enrolled_courses:
                continue

            for entry_key, entry_value in entries.items():
                if "entry" in entry_key:
                    exit_key = entry_key.replace("entry", "exit")
                    entry_dt = parse_datetime(entry_value.get("read_datetime"))
                    exit_dt = parse_datetime(entries.get(exit_key, {}).get("read_datetime"))

                    if not entry_dt:
                        continue

                    entry_date = entry_dt.date()
                    if entry_date.strftime("%A") != day:
                        continue

                    start_dt = combine_date_and_time(entry_date, start_time)
                    finish_dt = combine_date_and_time(entry_date, finish_time)

                    status = judge_attendance(entry_dt, exit_dt, start_dt, finish_dt)

                    col = entry_date.day + 1
                    row = int(student_index[1:]) + 1
                    worksheet.update_cell(row, col, status)

    print("処理が完了しました。")

if __name__ == "__main__":
    process_attendance_and_write_sheet()
