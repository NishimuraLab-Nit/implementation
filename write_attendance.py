import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseアプリの初期化（未初期化の場合のみ実行）
if not firebase_admin._apps:
    print("Firebaseの初期化を実行します。")
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Google Sheets API用のスコープを設定
print("Google Sheets APIのスコープを設定します。")
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
client = gspread.authorize(creds)

# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    print(f"Firebaseから'{path}'のデータを取得します。")
    ref = db.reference(path)
    data = ref.get()
    print(f"'{path}'のデータ: {data}")
    return data

# 時刻を分単位で計算する関数
def time_to_minutes(time_str):
    print(f"'{time_str}'を分に変換します。")
    time_obj = datetime.datetime.strptime(time_str, "%H:%M")
    minutes = time_obj.hour * 60 + time_obj.minute
    print(f"{time_str} は {minutes} 分です。")
    return minutes

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
    elif entry_minutes >= end_minutes:
        print("欠席")
        return "×"
    print("出席状況が認識できません。")
    return None

# Firebaseに退室時間を保存する関数
def save_exit_time_to_firebase(student_id, course_id, exit_time):
    print(f"Firebaseに退室時間を保存します: 学生ID={student_id}, コースID={course_id}, 退室時間={exit_time}")
    path = f"Students/attendance/students_id/{student_id}/exit{course_id}"
    ref = db.reference(path)
    ref.set({'read_datetime': exit_time.strftime("%Y-%m-%d %H:%M:%S")})
    print("退室時間をFirebaseに保存しました。")

# Firebaseに入室時間2を保存する関数
def save_entry2_time_to_firebase(student_id, course_id, entry2_time):
    print(f"Firebaseに入室時間2を保存します: 学生ID={student_id}, コースID={course_id}, 入室時間2={entry2_time}")
    path = f"Students/attendance/students_id{student_id}/entry{course_id}"
    ref = db.reference(path)
    ref.set({'read_datetime': entry2_time.strftime("%Y-%m-%d %H:%M:%S")})
    print("入室時間2をFirebaseに保存しました。")

# 出席を記録する関数
def record_attendance(students_data, courses_data):
    print("\n出席記録を開始します。")
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    student_index_data = students_data.get('student_info', {}).get('student_index', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        print(f"\n学生ID: {student_id}")
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。スキップします。")
            continue

        student_index = student_info['student_index']
        print(f"学生インデックス: {student_index}")
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', "").split(", ")

        for course_index, course_id in enumerate(course_ids, start=1):
            print(f"コースID: {course_id}")
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

            # 退室時間がない場合、終了時間1を設定して保存
            if not exit_time_str:
                exit_time = entry_time.replace(hour=end_minutes // 60, minute=end_minutes % 60)
                save_exit_time_to_firebase(student_id, course_id, exit_time)
                exit_minutes = end_minutes
                print(f"退室時間がないため、終了時間1を設定しました: {exit_time}")
            else:
                exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
                exit_minutes = exit_time.hour * 60 + exit_time.minute

                # 退室時間が終了時間＋5分以降の場合
                if exit_minutes > end_minutes + 5:
                    exit_time = entry_time.replace(hour=end_minutes // 60, minute=end_minutes % 60)
                    save_exit_time_to_firebase(student_id, course_id, exit_time)
                    print(f"退室時間が終了時間+5分以降のため終了時間1を設定しました: {exit_time}")

                    # 入室時間2を「終了時間+10分」に設定して保存
                    entry2_time = entry_time.replace(hour=(end_minutes + 10) // 60, minute=(end_minutes + 10) % 60)
                    save_entry2_time_to_firebase(student_id, course_id, entry2_time)
        
                    result = determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes)

            # 出席結果を出力
            print(f"学生 {student_id} のコース {course_id} の判定結果: {result}")

# Firebaseからデータを取得して出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
