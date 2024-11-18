import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseアプリの初期化（未初期化の場合のみ実行）
if not firebase_admin._apps:
    # サービスアカウントの認証情報を設定
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    # Firebaseアプリを初期化
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Google Sheets API用のスコープを設定
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
# Googleサービスアカウントから資格情報を取得
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
client = gspread.authorize(creds)

# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    ref = db.reference(path)
    return ref.get()

# 出席を確認しマークする関数
def check_and_mark_attendance(attendance, course, sheet, entry_label, student_name):
    # 入室時間を取得
    entry_time_str = attendance.get(entry_label, {}).get('read_datetime')
    if not entry_time_str:
        return False

    # 入室時間を日付オブジェクトに変換
    entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
    entry_day = entry_time.strftime("%A")
    entry_minutes = entry_time.hour * 60 + entry_time.minute

    # コースの日と一致するか確認
    if course['schedule']['day'] != entry_day:
        return False

    # コースの開始時間を取得
    start_time_str = course['schedule']['time'].split('-')[0]
    start_time = datetime.datetime.strptime(start_time_str, "%H:%M")
    start_minutes = start_time.hour * 60 + start_time.minute

    # 入室時間が許容範囲内か確認
    if abs(entry_minutes - start_minutes) <= 5:
        # 正しいセル位置を計算して更新
        column = entry_time.day + 1
        cell = sheet.find(student_name)
        if cell:
            sheet.update_cell(cell.row, column, "○")
        print(f"出席確認: {course['class_name']} - {entry_label} for {student_name}")
        return True
    return False

# 出席を記録する関数
def record_attendance(students_data, courses_data):
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_number', {})
    student_info_data = students_data.get('student_info', {}).get('student_number', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            raise ValueError(f"Student {student_id} information not found.")

        student_number = student_info.get('student_number')
        student_name = student_info_data.get(student_number, {}).get('student_name')

        if student_number not in enrollment_data:
            raise ValueError(f"Student number {student_number} enrollment not found.")

        course_ids = [cid for cid in enrollment_data[student_number].get('course_id', []) if cid is not None]

        for course_id in course_ids:
            if course_id >= len(courses_list):
                raise ValueError(f"Invalid course ID {course_id} found.")

            course = courses_list[course_id]
            if not course:
                raise ValueError(f"Course ID {course_id} not found.")

            sheet_id = course.get('course_sheet_id')
            if not sheet_id:
                raise ValueError(f"Spreadsheet ID not found for course ID {course_id}.")

            # Open the spreadsheet
            sheet = client.open_by_key(sheet_id).sheet1
            sheet.update_cell(1, 1, "Attendance List")

            # Add student name to the sheet if not already present
            students_in_sheet = sheet.col_values(1)
            if student_name not in students_in_sheet:
                sheet.append_row([student_name])

            # Check attendance for entries
            check_and_mark_attendance(attendance, course, sheet, 'entry1', student_name)
            if 'entry2' in attendance:
                check_and_mark_attendance(attendance, course, sheet, 'entry2', student_name)

# Firebaseからデータを取得し、出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
