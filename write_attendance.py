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

# 時刻を分単位で計算する関数
def time_to_minutes(time_str):
    time_obj = datetime.datetime.strptime(time_str, "%H:%M")
    return time_obj.hour * 60 + time_obj.minute

# 出席判定ロジック
def determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes):
    if entry_minutes <= start_minutes + 5:  # 入室が「開始時間＋5分以内」
        if exit_minutes >= end_minutes - 5:  # 退室が「終了時間−5分以降」
            return "○"  # 正常出席
        elif exit_minutes < end_minutes - 5:  # 退室が早い場合
            early_minutes = end_minutes - 5 - exit_minutes
            return f"△早{early_minutes}分"  # 早退
    elif entry_minutes > start_minutes + 5:  # 遅刻の場合
        if exit_minutes >= end_minutes - 5:  # 退室が「終了時間−5分以降」
            late_minutes = entry_minutes - (start_minutes + 5)
            return f"△遅{late_minutes}分"  # 遅刻
    elif entry_minutes >= end_minutes:  # 入室が「終了時間」以降
        return "×"  # 欠席
    return None

# 出席を記録する関数
def record_attendance(students_data, courses_data):
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    student_index_data = students_data.get('student_info', {}).get('student_index', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。スキップします。")
            continue

        student_index = student_info['student_index']
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', "").split(", ")  # 配列として扱う

        sheet_id = student_index_data.get(student_index, {}).get('sheet_id')
        if not sheet_id:
            print(f"学生インデックス {student_index} に対応するスプレッドシートIDが見つかりません。")
            continue

        try:
            sheet = client.open_by_key(sheet_id)
        except Exception as e:
            print(f"スプレッドシート {sheet_id} を開けません: {e}")
            continue

        entry_index = 1  # entryN, exitNのインデックス
        for course_id in course_ids:
            while True:  # entryN, exitNを順に処理
                try:
                    course_id_int = int(course_id)
                    course = courses_list[course_id_int]
                    if not course:
                        raise ValueError(f"コースID {course_id} に対応する授業が見つかりません。")
                except (ValueError, IndexError):
                    print(f"無効なコースID {course_id} が見つかりました。スキップします。")
                    break

                # entryNとexitNの判定
                entry_key = f'entry{entry_index}'
                exit_key = f'exit{entry_index}'
                entry_time_str = attendance.get(entry_key, {}).get('read_datetime')
                exit_time_str = attendance.get(exit_key, {}).get('read_datetime', None)

                if not entry_time_str:
                    print(f"学生 {student_id} の {entry_key} データが見つかりません。次のコースに移行します。")
                    break  # 次のコースIDに移行

                entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
                entry_minutes = entry_time.hour * 60 + entry_time.minute

                start_time_str, end_time_str = course.get('schedule', {}).get('time', '').split('~')
                start_minutes = time_to_minutes(start_time_str)
                end_minutes = time_to_minutes(end_time_str)

                if exit_time_str:
                    exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
                    exit_minutes = exit_time.hour * 60 + exit_time.minute
                else:
                    exit_minutes = end_minutes

                result = determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes)

                try:
                    entry_month = entry_time.strftime("%Y-%m")
                    sheet_to_update = sheet.worksheet(entry_month)
                except gspread.exceptions.WorksheetNotFound:
                    print(f"シート '{entry_month}' が見つかりません。スキップします。")
                    break

                row = course_id_int + entry_index
                column = entry_time.day + 1
                sheet_to_update.update_cell(row, column, result)
                print(f"出席記録: {course['class_name']} - {result}")

                # 次のentryN, exitNに進む
                entry_index += 1

# Firebaseからデータを取得して出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
