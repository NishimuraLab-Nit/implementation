import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseアプリの初期化
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
        return time_obj.hour * 60 + time_obj.minute
    except Exception as e:
        print(f"時刻変換中にエラーが発生しました: {e}")
        return None

# Firebaseに時刻を保存
def save_time_to_firebase(path, time_obj):
    try:
        print(f"Firebaseにデータを保存します: {path} - {time_obj}")
        ref = db.reference(path)
        ref.set({'read_datetime': time_obj.strftime("%Y-%m-%d %H:%M:%S")})
        print(f"{path} に保存しました。")
    except Exception as e:
        print(f"Firebaseへの保存中にエラーが発生しました: {e}")

# 出席判定ロジック
def determine_attendance_with_transition(entry_minutes, exit_minutes, start_minutes, end_minutes, student_id, course_index):
    print(f"出席判定: entry_minutes={entry_minutes}, exit_minutes={exit_minutes}, start_minutes={start_minutes}, end_minutes={end_minutes}")
    transition_occurred = False

    if exit_minutes > end_minutes + 5:  # 退室時間が終了時間+5分以降の場合
        print("退室時間が終了時間+5分以降です。処理を開始します。")
        
        final_exit_time = end_minutes
        final_exit_obj = datetime.datetime.now().replace(hour=final_exit_time // 60, minute=final_exit_time % 60)
        save_time_to_firebase(f"Students/attendance/student_id/{student_id}/exit{course_index}", final_exit_obj)

        new_entry_time = end_minutes + 5
        new_entry_obj = datetime.datetime.now().replace(hour=new_entry_time // 60, minute=new_entry_time % 60)
        save_time_to_firebase(f"Students/attendance/student_id/{student_id}/entry{course_index + 1}", new_entry_obj)

        new_exit_obj = datetime.datetime.now().replace(hour=exit_minutes // 60, minute=exit_minutes % 60)
        save_time_to_firebase(f"Students/attendance/student_id/{student_id}/exit{course_index + 1}", new_exit_obj)
        
        print("退室時間1を終了時間1に設定し、入室時間2と退室時間2を保存しました。")
        transition_occurred = True
        return "○", transition_occurred

    if entry_minutes <= start_minutes + 5:
        if exit_minutes >= end_minutes - 5:
            return "○", transition_occurred
        elif exit_minutes < end_minutes - 5:
            early_minutes = end_minutes - 5 - exit_minutes
            return f"△早{early_minutes}分", transition_occurred
    elif entry_minutes > start_minutes + 5:
        if exit_minutes >= end_minutes - 5:
            late_minutes = entry_minutes - (start_minutes + 5)
            return f"△遅{late_minutes}分", transition_occurred
    return "×", transition_occurred

# 出席を記録
def record_attendance(students_data, courses_data):
    if not students_data or not courses_data:
        print("学生データまたはコースデータが存在しません。")
        return

    print("\n出席記録を開始します。")
    attendance_data = students_data.get('attendance', {}).get('student_id', {})
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

        course_index = 1  # コースの処理用インデックス
        while course_index <= len(course_ids):
            course_id = course_ids[course_index - 1]
            print(f"コースID: {course_id}")
            try:
                course = courses_list[int(course_id)]
            except (ValueError, IndexError):
                print(f"無効なコースID {course_id} が見つかりました。スキップします。")
                course_index += 1
                continue

            schedule = course.get('schedule', {}).get('time', '').split('~')
            if len(schedule) != 2:
                print(f"コース {course_id} のスケジュール情報が不完全です。スキップします。")
                course_index += 1
                continue

            start_minutes = time_to_minutes(schedule[0])
            end_minutes = time_to_minutes(schedule[1])

            entry_key = f'entry{course_index}'
            exit_key = f'exit{course_index}'

            entry_time_str = attendance.get(entry_key, {}).get('read_datetime')
            if not entry_time_str:
                print(f"学生 {student_id} の {entry_key} データが見つかりません。次のコースに移行します。")
                course_index += 1
                continue

            entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
            entry_minutes = time_to_minutes(entry_time.strftime("%H:%M"))

            exit_time_str = attendance.get(exit_key, {}).get('read_datetime')
            if not exit_time_str:
                exit_time = entry_time.replace(hour=end_minutes // 60, minute=end_minutes % 60)
                save_time_to_firebase(f"Students/attendance/student_id/{student_id}/exit{course_index}", exit_time)
                exit_minutes = end_minutes
            else:
                exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
                exit_minutes = time_to_minutes(exit_time.strftime("%H:%M"))

            result, transition = determine_attendance_with_transition(entry_minutes, exit_minutes, start_minutes, end_minutes, student_id, course_index)
            print(f"学生 {student_id} のコース {course_id} の判定結果: {result}")

            if transition:
                print(f"移行が発生しました。次のコース（ID: {course_index + 1}）を処理します。")
            course_index += 1

# メイン処理
def main():
    initialize_firebase()
    client = initialize_google_sheets()
    students_data = get_data_from_firebase('Students')
    courses_data = get_data_from_firebase('Courses')
    record_attendance(students_data, courses_data)

if __name__ == "__main__":
    main()
