import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# Firebaseアプリの初期化（未初期化の場合のみ実行）
if not firebase_admin._apps:
    print("Firebaseの初期化を実行します。")
    try:
        cred = credentials.Certificate('/tmp/firebase_service_account.json')
        firebase_admin.initialize_app(cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })
    except Exception as e:
        print(f"Firebaseの初期化に失敗しました: {e}")
        raise


# Google Sheets API用のスコープを設定
try:
    print("Google Sheets APIのスコープを設定します。")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
    client = gspread.authorize(creds)
except Exception as e:
    print(f"Google Sheets APIの初期化に失敗しました: {e}")
    raise


# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    try:
        print(f"Firebaseから'{path}'のデータを取得します。")
        ref = db.reference(path)
        data = ref.get()
        if data is None:
            raise ValueError(f"'{path}' のデータが存在しません。")
        print(f"'{path}'のデータ取得成功")
        return data
    except Exception as e:
        print(f"Firebaseからデータ取得中にエラーが発生しました: {e}")
        return {}


# 時刻を分単位で計算する関数
def time_to_minutes(time_str):
    try:
        time_obj = datetime.datetime.strptime(time_str, "%H:%M")
        return time_obj.hour * 60 + time_obj.minute
    except Exception as e:
        print(f"時刻変換エラー: {time_str} - {e}")
        return None


# 出席判定ロジック
def determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes):
    if entry_minutes <= start_minutes + 5:
        if exit_minutes >= end_minutes - 5:
            return "○"  # 正常出席
        early_minutes = max(0, end_minutes - 5 - exit_minutes)
        return f"△早{early_minutes}分" if early_minutes > 0 else "○"
    elif entry_minutes > start_minutes + 5:
        late_minutes = entry_minutes - (start_minutes + 5)
        return f"△遅{late_minutes}分"
    return "×"  # 欠席


# Firebaseに退室時間を保存する関数
def save_exit_time_to_firebase(student_id, course_id, exit_time):
    try:
        path = f"Students/attendance/students_id/{student_id}/exit{course_id}"
        ref = db.reference(path)
        ref.set({'read_datetime': exit_time.strftime("%Y-%m-%d %H:%M:%S")})
        print(f"退室時間をFirebaseに保存しました: {exit_time}")
    except Exception as e:
        print(f"退室時間の保存に失敗しました: {e}")


# Firebaseに入室時間2を保存する関数
def save_entry2_time_to_firebase(student_id, course_id, entry2_time):
    try:
        path = f"Students/attendance/students_id/{student_id}/entry{course_id}"
        ref = db.reference(path)
        ref.set({'read_datetime': entry2_time.strftime("%Y-%m-%d %H:%M:%S")})
        print(f"入室時間2をFirebaseに保存しました: {entry2_time}")
    except Exception as e:
        print(f"入室時間2の保存に失敗しました: {e}")


# 出席を記録する関数
def record_attendance(students_data, courses_data):
    print("\n出席記録を開始します。")
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。スキップします。")
            continue

        student_index = student_info['student_index']
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', "").split(", ")

        for course_index, course_id in enumerate(course_ids, start=1):
            course = courses_list[int(course_id)]
            if not course:
                print(f"無効なコースID {course_id} が見つかりました。スキップします。")
                continue

            schedule = course.get('schedule', {}).get('time', '').split('~')
            if len(schedule) != 2:
                print(f"コース {course_id} のスケジュール情報が不完全です。スキップします。")
                continue

            start_time_str, end_time_str = schedule
            start_minutes = time_to_minutes(start_time_str)
            end_minutes = time_to_minutes(end_time_str)

            entry_key = f'entry{course_index}'
            exit_key = f'exit{course_index}'

            entry_time_str = attendance.get(entry_key, {}).get('read_datetime')
            exit_time_str = attendance.get(exit_key, {}).get('read_datetime', None)

            if not entry_time_str:
                print(f"学生 {student_id} の {entry_key} データが見つかりません。次のコースに移行します。")
                continue

            entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
            entry_minutes = entry_time.hour * 60 + entry_time.minute

            # 退室時間がない場合、終了時間を設定して保存
            if not exit_time_str:
                exit_time = entry_time.replace(hour=end_minutes // 60, minute=end_minutes % 60)
                save_exit_time_to_firebase(student_id, course_id, exit_time)
                exit_minutes = end_minutes
            else:
                exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
                exit_minutes = exit_time.hour * 60 + exit_time.minute

            # 出席状況の判定
            result = determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes)
            print(f"学生 {student_id} のコース {course_id} の判定結果: {result}")


# Firebaseからデータを取得して出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
