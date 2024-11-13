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

def check_and_mark_attendance(attendance, course, sheet, entry_label):
    entry_time_str = attendance.get(entry_label, {}).get('read_datetime')
    
    if not entry_time_str:
        return False

    entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
    entry_day = entry_time.strftime("%A")
    entry_minutes = entry_time.hour * 60 + entry_time.minute

    if course['schedule']['day'] != entry_day:
        return False
    
    start_time_str = course['schedule']['time'].split('-')[0]
    start_time = datetime.datetime.strptime(start_time_str, "%H:%M")
    start_minutes = start_time.hour * 60 + start_time.minute

    if abs(entry_minutes - start_minutes) <= 5:
        row = course['course_id'] + 1
        column = entry_time.day + 1
        sheet.update_cell(row, column, "○")
        print(f"出席確認: {course['class_name']} - {entry_label}")
        return True
    return False

def record_attendance(students_data, courses_data):
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_number', {})
    item_data = students_data.get('item', {}).get('student_number', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            raise ValueError(f"学生 {student_id} の情報が見つかりません。")
        
        student_number = student_info.get('student_number')
        if student_number not in enrollment_data:
            raise ValueError(f"学生番号 {student_number} の登録クラスが見つかりません。")

        course_ids = [cid for cid in enrollment_data[student_number].get('course_id', []) if cid is not None]
        
        sheet_id = item_data.get(student_number, {}).get('sheet_id')
        if not sheet_id:
            raise ValueError(f"学生番号 {student_number} に対応するスプレッドシートIDが見つかりません。")
        
        sheet = client.open_by_key(sheet_id).sheet1

        for course_id in course_ids:
            course = courses_list[course_id]
            if not course:
                raise ValueError(f"コースID {course_id} に対応する授業が見つかりません。")
            
            marked = check_and_mark_attendance(attendance, course, sheet, 'entry1')

            if 'entry2' in attendance:
                check_and_mark_attendance(attendance, course, sheet, 'entry2')

# Firebaseからデータを取得し、出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
