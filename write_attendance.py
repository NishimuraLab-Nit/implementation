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
        print("Firebaseの初期化に成功しました。")
    except Exception as e:
        print(f"Firebaseの初期化に失敗しました: {e}")
        raise

# Google Sheets API用のスコープを設定
try:
    print("Google Sheets APIのスコープを設定します。")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
    client = gspread.authorize(creds)
    print("Google Sheets APIの初期化に成功しました。")
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
            print(f"'{path}' のデータは存在しません。")
            return {}
        print(f"'{path}'のデータ取得成功: {len(data)}件取得")
        return data
    except Exception as e:
        print(f"Firebaseからデータ取得中にエラーが発生しました: {e}")
        return {}

# 時刻を分単位で計算する関数
def time_to_minutes(time_str):
    try:
        print(f"時刻 {time_str} を分単位に変換します。")
        time_obj = datetime.datetime.strptime(time_str, "%H:%M")
        minutes = time_obj.hour * 60 + time_obj.minute
        print(f"時刻 {time_str} は {minutes} 分です。")
        return minutes
    except Exception as e:
        print(f"時刻変換エラー: {time_str} - {e}")
        return None

# 出席判定ロジック
def determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes):
    print(f"出席判定: 入室={entry_minutes}分, 退室={exit_minutes}分, 開始={start_minutes}分, 終了={end_minutes}分")
    if entry_minutes <= start_minutes + 5:
        if exit_minutes >= end_minutes - 5:
            print("判定結果: 正常出席 (○)")
            return "○"  # 正常出席
        early_minutes = max(0, end_minutes - 5 - exit_minutes)
        result = f"△早{early_minutes}分" if early_minutes > 0 else "○"
        print(f"判定結果: {result}")
        return result
    elif entry_minutes > start_minutes + 5:
        late_minutes = entry_minutes - (start_minutes + 5)
        result = f"△遅{late_minutes}分"
        print(f"判定結果: {result}")
        return result
    print("判定結果: 欠席 (×)")
    return "×"  # 欠席

# Firebaseにデータを保存する汎用関数
def save_to_firebase(path, data, description):
    try:
        print(f"Firebaseにデータを保存します: {description} -> {path}")
        ref = db.reference(path)
        ref.set(data)
        print(f"{description} をFirebaseに保存しました: {data}")
    except Exception as e:
        print(f"{description} の保存に失敗しました: {e}")

# Firebaseに退室時間を保存する関数
def save_exit_time_to_firebase(student_id, course_id, exit_time):
    try:
        print(f"Firebaseに退室時間を保存します: 学生ID={student_id}, コースID={course_id}, 退室時間={exit_time}")
        path = f"Students/attendance/students_id/{student_id}/exit{course_id}"
        ref = db.reference(path)
        ref.set({'read_datetime': exit_time.strftime("%Y-%m-%d %H:%M:%S")})
        print("退室時間をFirebaseに保存しました。")
    except Exception as e:
        print(f"退室時間の保存に失敗しました: {e}")

# Firebaseに入室時間2を保存する関数
def save_entry2_time_to_firebase(student_id, course_id, entry2_time):
    try:
        print(f"Firebaseに入室時間2を保存します: 学生ID={student_id}, コースID={course_id}, 入室時間2={entry2_time}")
        path = f"Students/attendance/students_id/{student_id}/entry2{course_id}"
        ref = db.reference(path)
        ref.set({'read_datetime': entry2_time.strftime("%Y-%m-%d %H:%M:%S")})
        print("入室時間2をFirebaseに保存しました。")
    except Exception as e:
        print(f"入室時間2の保存に失敗しました: {e}")

# 出席を記録する関数
def record_attendance(students_data, courses_data):
    print("\n=== 出席記録を開始します ===")
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    courses_list = courses_data.get('course_id', [])

    print(f"学生データ数: {len(attendance_data)}, コースデータ数: {len(courses_list)}")

    for student_id, attendance in attendance_data.items():
        print(f"学生ID {student_id} の出席処理を開始します。")
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。スキップします。")
            continue

        student_index = student_info['student_index']
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', "").split(", ")
        print(f"学生 {student_id} のコース登録数: {len(course_ids)}")

        for course_index, course_id in enumerate(course_ids, start=1):
            print(f"コース {course_id} の処理を開始します。")
            course = courses_list[int(course_id)] if course_id.isdigit() and int(course_id) < len(courses_list) else None
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

            print(f"学生 {student_id} のコース {course_id}: 入室キー={entry_key}, 退室キー={exit_key}")

            if not entry_time_str:
                print(f"学生 {student_id} の {entry_key} データが見つかりません。次のコースに移行します。")
                continue

            try:
                entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
                entry_minutes = entry_time.hour * 60 + entry_time.minute
            except ValueError as e:
                print(f"入室時刻の解析に失敗しました: {entry_time_str} ({e})。スキップします。")
                continue

            if not exit_time_str:
                print(f"学生 {student_id} の退室時間が見つかりません。デフォルトの退室時間を設定します。")
                exit_time = entry_time.replace(hour=end_minutes // 60, minute=end_minutes % 60)
                entry2_time = entry_time.replace(hour=(end_minutes + 10) // 60, minute=(end_minutes + 10) % 60)
                save_exit_time_to_firebase(student_id, course_id, exit_time)
                save_entry2_time_to_firebase(student_id, course_id, entry2_time)
                exit_minutes = end_minutes
            else:
                try:
                    exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
                    exit_minutes = exit_time.hour * 60 + exit_time.minute
                except ValueError as e:
                    print(f"退室時刻の解析に失敗しました: {exit_time_str} ({e})。デフォルト値を使用します。")
                    exit_minutes = end_minutes

            result = determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes)
            print(f"学生 {student_id} のコース {course_id} の判定結果: {result}")

    print("=== 出席記録を終了します ===")

# Firebaseからデータを取得して出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
