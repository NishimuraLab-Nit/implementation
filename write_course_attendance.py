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
# メインフロー
# ---------------------
def process_attendance_and_write_sheet():
    courses_data = get_data_from_firebase("Courses/course_id")
    if not courses_data:
        print("Courses データがありません。終了します。")
        return

    for course_id, course_info in enumerate(courses_data):
        if course_id == 0 or not course_info:
            continue

        course_sheet_id = course_info.get("course_sheet_id")
        schedule_info = course_info.get("schedule", {})
        time_range_str = schedule_info.get("time", "")
        start_time, end_time = parse_hhmm_range(time_range_str)
        if not course_sheet_id or not start_time or not end_time:
            continue

        enrollment_path = f"Students/enrollment/course_id/{course_id}"
        enrollment_data = get_data_from_firebase(enrollment_path)

        if not enrollment_data:
            continue

        for student_index, _ in enrollment_data.items():
            student_info_path = f"Students/student_info/{student_index}"
            student_info = get_data_from_firebase(student_info_path)

            if not student_info:
                continue

            student_id = student_info.get("student_id")
            attendance_path = f"Students/attendance/student_id/{student_id}"
            attendance_data = get_data_from_firebase(attendance_path)

            if not attendance_data:
                continue

            sheet = gclient.open_by_key(course_sheet_id)
            sheet_name = datetime.datetime.now().strftime("%Y-%m")
            try:
                worksheet = sheet.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sheet.add_worksheet(title=sheet_name, rows=50, cols=50)

            for entry_key, entry_data in attendance_data.items():
                entry_dt = parse_datetime(entry_data.get("read_datetime"))
                if not entry_dt:
                    continue

                start_dt = combine_date_and_time(entry_dt.date(), start_time)
                end_dt = combine_date_and_time(entry_dt.date(), end_time)

                status = "×" if entry_dt >= end_dt else "○"
                day = entry_dt.day

                row = int(student_index.split("E")[-1]) + 1
                col = day + 1
                worksheet.update_cell(row, col, status)

    print("=== 出席判定処理＆シート書き込み完了 ===")

if __name__ == "__main__":
    process_attendance_and_write_sheet()
