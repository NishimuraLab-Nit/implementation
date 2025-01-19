import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseアプリの初期化（未初期化の場合のみ実行）
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Google Sheets API用のスコープを設定
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Googleサービスアカウントから資格情報を取得
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
client = gspread.authorize(creds)

def get_data_from_firebase(path):
    ref = db.reference(path)
    return ref.get()

def check_and_mark_attendance(attendance, course, sheet, entry_label, exit_label, course_id, next_course=None):
    entry_time_str = attendance.get(entry_label, {}).get('read_datetime')
    exit_time_str = attendance.get(exit_label, {}).get('read_datetime', None)

    if not entry_time_str:
        return False

    entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
    exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S") if exit_time_str else None

    start_time_str, end_time_str = course.get('schedule', {}).get('time', '').split('~')
    start_time = datetime.datetime.strptime(start_time_str.strip(), "%H:%M")
    end_time = datetime.datetime.strptime(end_time_str.strip(), "%H:%M")

    result = ""

    # 欠席判定
    if entry_time >= end_time:
        result = "×"  # 欠席

    # 正常出席判定
    elif entry_time <= start_time + datetime.timedelta(minutes=5):
        if exit_time and exit_time <= end_time + datetime.timedelta(minutes=5):
            result = "○"  # 正常出席
        elif exit_time:
            exit_time = end_time  # 退室時間未入力時の処理

    # 早退判定
    elif entry_time <= start_time + datetime.timedelta(minutes=5) and exit_time and exit_time < end_time - datetime.timedelta(minutes=5):
        early_minutes = (end_time - exit_time).seconds // 60
        result = f"△早{early_minutes}分"

    # 遅刻判定
    elif entry_time > start_time + datetime.timedelta(minutes=5) and exit_time and exit_time <= end_time + datetime.timedelta(minutes=5):
        late_minutes = (entry_time - (start_time + datetime.timedelta(minutes=5))).seconds // 60
        result = f"△遅{late_minutes}分"

    # 次のコースへ移行
    if next_course:
        next_start_time_str, next_end_time_str = next_course.get('schedule', {}).get('time', '').split('~')
        next_start_time = datetime.datetime.strptime(next_start_time_str.strip(), "%H:%M")
        next_end_time = datetime.datetime.strptime(next_end_time_str.strip(), "%H:%M")

        if exit_time and exit_time >= next_start_time - datetime.timedelta(minutes=5):
            result = "○"  # 次のコースも正常出席

    try:
        entry_month = entry_time.strftime("%Y-%m")
        sheet_to_update = sheet.worksheet(entry_month)
    except gspread.exceptions.WorksheetNotFound:
        print(f"シート '{entry_month}' が見つかりません。スキップします。")
        return False

    row = int(course_id) + 1
    column = entry_time.day + 1
    sheet_to_update.update_cell(row, column, result)
    print(f"出席記録: {course['class_name']} - {entry_label} - シート: {entry_month} - 結果: {result}")
    return True

def record_attendance(students_data, courses_data):
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    student_index_data = students_data.get('student_info', {}).get('student_index', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。")
            continue

        student_index = student_info.get('student_index')
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', []).split(",")  # 修正: カンマ区切りで分割

        sheet_id = student_index_data.get(student_index, {}).get('sheet_id')
        if not sheet_id:
            print(f"学生インデックス {student_index} に対応するスプレッドシートIDが見つかりません。")
            continue

        try:
            sheet = client.open_by_key(sheet_id)
        except Exception as e:
            print(f"スプレッドシート {sheet_id} を開けません: {e}")
            continue

        for i, course_id in enumerate(course_ids):
            try:
                course_id_int = int(course_id)
                course = courses_list[course_id_int]
                next_course = courses_list[course_id_int + 1] if course_id_int + 1 < len(courses_list) else None
                if not course:
                    raise ValueError(f"コースID {course_id} に対応する授業が見つかりません。")
            except (ValueError, IndexError):
                print(f"無効なコースID {course_id} が見つかりました。スキップします。")
                continue

            if check_and_mark_attendance(attendance, course, sheet, 'entry1', 'exit1', course_id, next_course):
                continue

            if 'entry2' in attendance:
                check_and_mark_attendance(attendance, course, sheet, 'entry2', 'exit2', course_id, next_course)

# Firebaseからデータを取得し、出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
