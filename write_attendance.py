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

# 退室時間の判定を含む出席記録ロジック
def process_exit_time(exit_time_str, end_minutes, exit_key, attendance, course_id, firebase_path):
    # 退室時間が存在しない場合の処理
    if not exit_time_str:
        print(f"{exit_key} が存在しません。終了時間1を退室時間1に設定します。")
        exit_minutes = end_minutes
        attendance[exit_key] = {'read_datetime': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        db.reference(firebase_path).child(course_id).child(exit_key).set(attendance[exit_key])
        print(f"Firebaseに退室時間1を保存しました: {attendance[exit_key]}")
        return "○", exit_minutes

    # 退室時間が「終了時間1 + 5分以降」の場合の処理
    exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
    exit_minutes = exit_time.hour * 60 + exit_time.minute
    if exit_minutes > end_minutes + 5:
        print(f"退室時間 {exit_time_str} が終了時間1 + 5分以降です。終了時間1を退室時間1として記録します。")
        exit_minutes = end_minutes
        attendance[exit_key] = {'read_datetime': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        db.reference(firebase_path).child(course_id).child(exit_key).set(attendance[exit_key])
        print(f"Firebaseに修正された退室時間1を保存しました: {attendance[exit_key]}")
        return "○", exit_minutes

    return None, exit_minutes

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

        firebase_path = f"Students/attendance/students_id/{student_id}"

        for course_id in course_ids:
            print(f"コースID: {course_id}")
            course = courses_list.get(int(course_id), {})
            if not course:
                print(f"コースID {course_id} が無効です。スキップします。")
                continue

            entry_key = f'entry{course_id}'
            exit_key = f'exit{course_id}'
            entry_time_str = attendance.get(entry_key, {}).get('read_datetime')
            exit_time_str = attendance.get(exit_key, {}).get('read_datetime', None)

            if not entry_time_str:
                print(f"学生 {student_id} の {entry_key} データが見つかりません。次のコースに移行します。")
                continue

            entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
            entry_minutes = entry_time.hour * 60 + entry_time.minute

            start_time_str, end_time_str = course.get('schedule', {}).get('time', '').split('~')
            start_minutes = time_to_minutes(start_time_str)
            end_minutes = time_to_minutes(end_time_str)

            result, exit_minutes = process_exit_time(exit_time_str, end_minutes, exit_key, attendance, course_id, firebase_path)
            if not result:
                result = determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes)

            print(f"コース {course_id} の出席結果: {result}")
