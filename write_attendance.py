import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def initialize_firebase(service_account_path, database_url):
    """Firebaseを初期化します。"""
    if not firebase_admin._apps:
        print("Firebaseを初期化しています...")
        cred = credentials.Certificate(service_account_path)
        firebase_admin.initialize_app(cred, {'databaseURL': database_url})
        print("Firebaseの初期化が完了しました。")

def initialize_google_sheets(service_account_path, scopes):
    """Google Sheets APIを初期化します。"""
    print("Google Sheets APIを初期化しています...")
    creds = ServiceAccountCredentials.from_json_keyfile_name(service_account_path, scopes)
    print("Google Sheets APIの初期化が完了しました。")
    return gspread.authorize(creds)

def get_data_from_firebase(path):
    """指定されたパスからFirebaseデータを取得します。"""
    print(f"Firebaseからデータを取得しています: {path}")
    ref = db.reference(path)
    data = ref.get()
    print(f"{path}から取得したデータ: {data}")
    return data

def time_to_minutes(time_str):
    """時刻文字列（HH:MM）を分単位に変換します。"""
    print(f"時刻文字列を分に変換します: {time_str}")
    time_obj = datetime.datetime.strptime(time_str, "%H:%M")
    minutes = time_obj.hour * 60 + time_obj.minute
    print(f"{time_str} は {minutes} 分です。")
    return minutes

def determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes):
    """出席状況を判定します。"""
    print(f"出席状況を判定しています: 入室時間={entry_minutes}分, 退室時間={exit_minutes}分, 開始時間={start_minutes}分, 終了時間={end_minutes}分")
    if entry_minutes <= start_minutes + 5:
        if exit_minutes >= end_minutes - 5:
            print("出席: 正常に出席しました。")
            return "○"
        elif exit_minutes < end_minutes - 5:
            early_minutes = end_minutes - 5 - exit_minutes
            print(f"早退: {early_minutes}分早退しました。")
            return f"△早{early_minutes}分"
    elif entry_minutes > start_minutes + 5:
        late_minutes = entry_minutes - (start_minutes + 5)
        if exit_minutes >= end_minutes - 5:
            print(f"遅刻: {late_minutes}分遅刻しました。")
            return f"△遲{late_minutes}分"
    print("欠席: 出席条件を満たしませんでした。")
    return "×"

def save_time_to_firebase(student_id, course_id, time_key, time_value):
    """Firebaseに時間データを保存します。"""
    path = f"Students/attendance/student_id/{student_id}/{time_key}{course_id}"
    db.reference(path).set({'read_datetime': time_value.strftime("%Y-%m-%d %H:%M:%S")})
    print(f"{time_key}時間をFirebaseに保存しました: {path} -> {time_value}")

def process_attendance_record(student_id, attendance, course, start_minutes, end_minutes):
    """学生とコースの出席記録を処理します。"""
    print(f"学生ID: {student_id}の出席記録を処理しています...")
    entry_time_str = attendance.get('read_datetime')
    if not entry_time_str:
        print(f"学生ID: {student_id}の入室時間が記録されていません。スキップします。")
        return None

    entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
    entry_minutes = entry_time.hour * 60 + entry_time.minute
    print(f"入室時間: {entry_time} ({entry_minutes}分)")

    exit_time_str = attendance.get('exit_time', {}).get('read_datetime')
    if not exit_time_str:
        exit_minutes = end_minutes
        exit_time = entry_time.replace(hour=end_minutes // 60, minute=end_minutes % 60)
        save_time_to_firebase(student_id, course['course_id'], 'exit', exit_time)
        print(f"退室時間が記録されていないため、終了時間を設定しました: {exit_time}")
    else:
        exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
        exit_minutes = exit_time.hour * 60 + exit_time.minute
        print(f"退室時間: {exit_time} ({exit_minutes}分)")

    if exit_minutes > end_minutes + 5:
        exit_time = entry_time.replace(hour=end_minutes // 60, minute=end_minutes % 60)
        save_time_to_firebase(student_id, course['course_id'], 'exit', exit_time)
        print(f"退室時間が終了時間+5分を超えたため、終了時間を再設定しました: {exit_time}")
        entry2_time = entry_time.replace(hour=(end_minutes + 10) // 60, minute=(end_minutes + 10) % 60)
        save_time_to_firebase(student_id, course['course_id'], 'entry2', entry2_time)
        print(f"入室時間2を設定しました: {entry2_time}")

    attendance_result = determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes)
    print(f"出席判定結果: {attendance_result}")
    return attendance_result

def record_attendance(students_data, courses_data):
    """すべての学生とコースの出席を記録します。"""
    print("出席記録を開始します...")
    for student_id, attendance in students_data.get('attendance', {}).get('students_id', {}).items():
        print(f"\n学生ID: {student_id}の出席データを処理します...")
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            print(f"学生ID: {student_id}の情報が見つかりません。スキップします。")
            continue

        for course_id in student_info.get('enrolled_courses', []):
            print(f"コースID: {course_id}の出席データを処理します...")
            course = courses_data.get('course_id', {}).get(course_id)
            if not course:
                print(f"無効なコースID: {course_id}が見つかりました。スキップします。")
                continue

            schedule = course.get('schedule', {}).get('time', '').split('~')
            if len(schedule) != 2:
                print(f"コースID: {course_id}のスケジュール情報が不完全です。スキップします。")
                continue

            start_minutes = time_to_minutes(schedule[0])
            end_minutes = time_to_minutes(schedule[1])

            result = process_attendance_record(student_id, attendance, course, start_minutes, end_minutes)
            if result is not None:
                print(f"学生ID: {student_id}, コースID: {course_id}の出席結果: {result}")

if __name__ == "__main__":
    # 初期化
    FIREBASE_CREDENTIALS = '/tmp/firebase_service_account.json'
    DATABASE_URL = 'https://test-51ebc-default-rtdb.firebaseio.com/'
    GCP_CREDENTIALS = '/tmp/gcp_service_account.json'
    SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    print("FirebaseとGoogle Sheetsの初期化を開始します...")
    initialize_firebase(FIREBASE_CREDENTIALS, DATABASE_URL)
    google_sheets_client = initialize_google_sheets(GCP_CREDENTIALS, SCOPES)

    print("Firebaseから学生データとコースデータを取得します...")
    students_data = get_data_from_firebase('Students')
    courses_data = get_data_from_firebase('Courses')

    print("出席記録の処理を開始します...")
    record_attendance(students_data, courses_data)
    print("出席記録の処理が完了しました。")
