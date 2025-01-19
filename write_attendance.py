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

def check_and_mark_attendance(attendance, course, sheet, entry_label, course_id, next_course=None):
    entry_time_str = attendance.get(entry_label, {}).get('read_datetime')
    if not entry_time_str:
        return False

    entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
    entry_minutes = entry_time.hour * 60 + entry_time.minute

    start_time_str = course.get('schedule', {}).get('time', '').split('~')[0]
    end_time_str = course.get('schedule', {}).get('time', '').split('~')[1]
    start_time = datetime.datetime.strptime(start_time_str, "%H:%M")
    end_time = datetime.datetime.strptime(end_time_str, "%H:%M")
    start_minutes = start_time.hour * 60 + start_time.minute
    end_minutes = end_time.hour * 60 + end_time.minute

    exit_time_str = attendance.get('exit', {}).get('read_datetime')
    if exit_time_str:
        exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
        exit_minutes = exit_time.hour * 60 + exit_time.minute
    else:
        exit_minutes = end_minutes

    # 正常出席の判定
    if abs(entry_minutes - start_minutes) <= 5 and exit_minutes >= end_minutes - 5:
        mark_attendance(sheet, entry_time, course_id, "○")
        return True

    # 早退の判定
    if abs(entry_minutes - start_minutes) <= 5 and exit_minutes < end_minutes - 5:
        early_leave_minutes = end_minutes - exit_minutes
        mark_attendance(sheet, entry_time, course_id, f"△早{early_leave_minutes}分")
        return True

    # 遅刻の判定
    if entry_minutes > start_minutes + 5 and exit_minutes >= end_minutes - 5:
        late_minutes = entry_minutes - start_minutes
        mark_attendance(sheet, entry_time, course_id, f"△遅{late_minutes}分")
        return True

    # 欠席の判定
    if entry_minutes > end_minutes:
        mark_attendance(sheet, entry_time, course_id, "×")
        return False

    return False

def mark_attendance(sheet, entry_time, course_id, mark):
    entry_month = entry_time.strftime("%Y-%m")
    row = int(course_id) + 1
    column = entry_time.day + 1
    try:
        sheet_to_update = sheet.worksheet(entry_month)
        sheet_to_update.update_cell(row, column, mark)
    except gspread.exceptions.WorksheetNotFound:
        print(f"シート '{entry_month}' が見つかりません。スキップします。")

# Firebaseからデータを取得し、出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
