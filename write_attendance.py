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
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
client = gspread.authorize(creds)

# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    ref = db.reference(path)
    return ref.get()

# 出席を確認しマークする関数
def check_and_mark_attendance(attendance, course, sheet, entry_label, course_id):
    entry_time_str = attendance.get(entry_label, {}).get('read_datetime')
    exit_time_str = attendance.get(entry_label, {}).get('exit_datetime')

    if not entry_time_str:
        return False

    entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
    entry_minutes = entry_time.hour * 60 + entry_time.minute

    start_time_str, end_time_str = course.get('schedule', {}).get('time', '').split('~')
    start_time = datetime.datetime.strptime(start_time_str, "%H:%M")
    end_time = datetime.datetime.strptime(end_time_str, "%H:%M")
    start_minutes = start_time.hour * 60 + start_time.minute
    end_minutes = end_time.hour * 60 + end_time.minute

    if exit_time_str:
        exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
        exit_minutes = exit_time.hour * 60 + exit_time.minute
    else:
        exit_time = None
        exit_minutes = None

    # 同教室エントリの処理
    if abs(entry_minutes - start_minutes) <= 5:
        if exit_minutes and exit_minutes >= end_minutes - 5:
            mark_attendance(sheet, course_id, entry_time, "○")
        elif exit_minutes and exit_minutes < end_minutes - 5:
            early_minutes = end_minutes - 5 - exit_minutes
            mark_attendance(sheet, course_id, entry_time, f"△早{early_minutes}分")
        elif not exit_time:
            mark_attendance(sheet, course_id, entry_time, "○")
            exit_time = end_time
    elif entry_minutes > start_minutes + 5:
        if exit_minutes and exit_minutes >= end_minutes - 5:
            late_minutes = entry_minutes - (start_minutes + 5)
            mark_attendance(sheet, course_id, entry_time, f"△遅{late_minutes}分")
        else:
            mark_attendance(sheet, course_id, entry_time, "×")
    
    # 次のスケジュールへの影響
    # ここに次のスケジュールへの判定ロジックを追加する

    return True

def mark_attendance(sheet, course_id, entry_time, status):
    entry_month = entry_time.strftime("%Y-%m")
    try:
        sheet_to_update = sheet.worksheet(entry_month)
    except gspread.exceptions.WorksheetNotFound:
        print(f"シート '{entry_month}' が見つかりません。スキップします。")
        return False

    row = int(course_id) + 1
    column = entry_time.day + 1
    sheet_to_update.update_cell(row, column, status)
    print(f"出席確認: {course_id} - シート: {entry_month} - ステータス: {status}")
    return True

# 出席を記録する関数
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
        course_ids = enrollment_info.get('course_id', [])

        sheet_id = student_index_data.get(student_index, {}).get('sheet_id')
        if not sheet_id:
            print(f"学生インデックス {student_index} に対応するスプレッドシートIDが見つかりません。")
            continue

        try:
            sheet = client.open_by_key(sheet_id)
        except Exception as e:
            print(f"スプレッドシート {sheet_id} を開けません: {e}")
            continue

        for course_id in course_ids:
            try:
                course_id_int = int(course_id)
                course = courses_list[course_id_int]
                if not course:
                    raise ValueError(f"コースID {course_id} に対応する授業が見つかりません。")
            except (ValueError, IndexError):
                print(f"無効なコースID {course_id} が見つかりました。スキップします。")
                continue

            if check_and_mark_attendance(attendance, course, sheet, 'entry1', course_id):
                continue

            if 'entry2' in attendance:
                check_and_mark_attendance(attendance, course, sheet, 'entry2', course_id)

# Firebaseからデータを取得し、出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
