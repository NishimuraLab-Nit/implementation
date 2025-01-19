import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseアプリの初期化
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Google Sheets API用のスコープ設定
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
client = gspread.authorize(creds)

def get_data_from_firebase(path):
    ref = db.reference(path)
    return ref.get()

def time_to_minutes(time_str):
    time_obj = datetime.datetime.strptime(time_str, "%H:%M")
    return time_obj.hour * 60 + time_obj.minute

def record_attendance(students_data, courses_data):
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    student_index_data = students_data.get('student_info', {}).get('student_index', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            continue

        student_index = student_info['student_index']
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', "").split(", ")

        sheet_id = student_index_data.get(student_index, {}).get('sheet_id')
        if not sheet_id:
            continue

        try:
            sheet = client.open_by_key(sheet_id)
        except Exception:
            continue

        entry_index = 1
        for course_id in course_ids:
            try:
                course_id_int = int(course_id)
                course = courses_list[course_id_int]
                if not course:
                    continue
            except (ValueError, IndexError):
                continue

            entry_key = f'entry{entry_index}'
            exit_key = f'exit{entry_index}'
            entry_time_str = attendance.get(entry_key, {}).get('read_datetime')
            exit_time_str = attendance.get(exit_key, {}).get('read_datetime', None)

            if not entry_time_str:
                continue

            entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
            entry_minutes = entry_time.hour * 60 + entry_time.minute

            start_time_str, end_time_str = course.get('schedule', {}).get('time', '').split('~')
            start_minutes = time_to_minutes(start_time_str)
            end_minutes = time_to_minutes(end_time_str)

            if exit_time_str:
                exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
                exit_minutes = exit_time.hour * 60 + exit_time.minute
            else:
                # 退室時間が存在しない場合の処理
                exit_minutes = end_minutes
                attendance[exit_key] = {'read_datetime': entry_time.strftime("%Y-%m-%d ") + f"{end_minutes // 60:02}:{end_minutes % 60:02}:00"}
                db.reference(f"Students/attendance/students_id/{student_id}/{exit_key}").set(attendance[exit_key])

            if exit_minutes >= end_minutes + 5:
                exit_minutes = end_minutes  # 終了時間を設定
                attendance[exit_key] = {'read_datetime': entry_time.strftime("%Y-%m-%d ") + f"{end_minutes // 60:02}:{end_minutes % 60:02}:00"}
                db.reference(f"Students/attendance/students_id/{student_id}/{exit_key}").set(attendance[exit_key])

            result = "○" if exit_minutes >= end_minutes else "△"

            try:
                entry_month = entry_time.strftime("%Y-%m")
                sheet_to_update = sheet.worksheet(entry_month)
            except gspread.exceptions.WorksheetNotFound:
                continue

            row = int(course_id) + 1
            column = entry_time.day + 1
            sheet_to_update.update_cell(row, column, result)

            entry_index += 1

# Firebaseからデータを取得して出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
