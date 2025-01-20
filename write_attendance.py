import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseアプリの初期化（未初期化の場合のみ実行）
def initialize_firebase():
    if not firebase_admin._apps:
        print("Firebaseの初期化を実行します。")
        cred = credentials.Certificate('/tmp/firebase_service_account.json')
        firebase_admin.initialize_app(cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })

# Google Sheets APIの初期化
def initialize_google_sheets():
    print("Google Sheets APIのスコープを設定します。")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
    return gspread.authorize(creds)

# Firebaseからデータを取得
def get_data_from_firebase(path):
    try:
        print(f"Firebaseから'{path}'のデータを取得します。")
        ref = db.reference(path)
        data = ref.get()
        print(f"'{path}'のデータ: {data}")
        return data
    except Exception as e:
        print(f"Firebaseからデータを取得中にエラーが発生しました: {e}")
        return None

# 時刻を分単位に変換
def time_to_minutes(time_str):
    try:
        print(f"'{time_str}'を分に変換します。")
        time_obj = datetime.datetime.strptime(time_str, "%H:%M")
        minutes = time_obj.hour * 60 + time_obj.minute
        print(f"{time_str} は {minutes} 分です。")
        return minutes
    except Exception as e:
        print(f"時刻変換中にエラーが発生しました: {e}")
        return None

# 出席判定ロジック
def determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes):
    print(f"出席判定: entry_minutes={entry_minutes}, exit_minutes={exit_minutes}, start_minutes={start_minutes}, end_minutes={end_minutes}")
    if entry_minutes <= start_minutes + 5:
        if exit_minutes >= end_minutes - 5:
            print("正常出席")
            return "○" 
        elif exit_minutes < end_minutes - 5:
            early_minutes = end_minutes - 5 - exit_minutes
            print(f"早退 {early_minutes} 分")
            return f"△早{early_minutes}分"
    elif entry_minutes > start_minutes + 5:
        if exit_minutes >= end_minutes - 5:
            late_minutes = entry_minutes - (start_minutes + 5)
            print(f"遅刻 {late_minutes} 分")
            return f"△遅{late_minutes}分"
    print("欠席")
    return "×"

# Firebaseに時刻を保存
def save_time_to_firebase(path, time_obj):
    try:
        print(f"Firebaseにデータを保存します: {path} - {time_obj}")
        ref = db.reference(path)
        ref.set({'read_datetime': time_obj.strftime("%Y-%m-%d %H:%M:%S")})
        print(f"{path} に保存しました。")
    except Exception as e:
        print(f"Firebaseへの保存中にエラーが発生しました: {e}")

# 出席を記録
def record_attendance(students_data, courses_data):
    if not students_data or not courses_data:
        print("学生データまたはコースデータが存在しません。")
        return

    print("\n出席記録を開始します。")
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    student_info_data = students_data.get('student_info', {}).get('student_id', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        print(f"\n学生ID: {student_id}")
        student_info = student_info_data.get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。スキップします。")
            continue

        student_index = student_info.get('student_index')
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', "").split(", ")

        for course_index, course_id in enumerate(course_ids, start=1):
            print(f"コースID: {course_id}")
            try:
                course = courses_list[int(course_id)]
            except (ValueError, IndexError):
                print(f"無効なコースID {course_id} が見つかりました。スキップします。")
                continue

            schedule = course.get('schedule', {}).get('time', '').split('~')
            if len(schedule) != 2:
                print(f"コース {course_id} のスケジュール情報が不完全です。スキップします。")
                continue

            start_minutes = time_to_minutes(schedule[0])
            end_minutes = time_to_minutes(schedule[1])

            entry_key = f'entry{course_index}'
            exit_key = f'exit{course_index}'

            entry_time_str = attendance.get(entry_key, {}).get('read_datetime')
            exit_time_str = attendance.get(exit_key, {}).get('read_datetime')

            if not entry_time_str:
                print(f"学生 {student_id} の {entry_key} データが見つかりません。次のコースに移行します。")
                continue

            entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
            entry_minutes = time_to_minutes(entry_time.strftime("%H:%M"))

            if not exit_time_str:
                exit_time = entry_time.replace(hour=end_minutes // 60, minute=end_minutes % 60)
                save_time_to_firebase(f"Students/attendance/student_id/{student_id}/exit{course_id}", exit_time)
                exit_minutes = end_minutes
            else:
                exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
                exit_minutes = time_to_minutes(exit_time.strftime("%H:%M"))

            result = determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes)
            print(f"学生 {student_id} のコース {course_id} の判定結果: {result}")

# メイン処理
def main():
    initialize_firebase()
    client = initialize_google_sheets()

    students_data = get_data_from_firebase('Students')
    courses_data = get_data_from_firebase('Courses')

    record_attendance(students_data, courses_data)

if __name__ == "__main__":
    main()
