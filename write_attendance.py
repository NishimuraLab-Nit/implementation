import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseアプリの初期化（未初期化の場合のみ実行）
if not firebase_admin._apps:
    print("Firebaseアプリを初期化します...")
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })
    print("Firebaseアプリが初期化されました。")

# Google Sheets API用のスコープを設定
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
client = gspread.authorize(creds)
print("Google Sheets APIの認証が完了しました。")

# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    print(f"Firebaseからデータを取得中: {path}")
    ref = db.reference(path)
    data = ref.get()
    if data:
        print(f"データ取得成功: {path}")
    else:
        print(f"データが見つかりません: {path}")
    return data

# 出席を確認し、必要に応じてデータを保存する関数
def check_and_handle_attendance(attendance, course, course_id):
    try:
        print(f"コースID {course_id} の出席を確認中...")
        # 入室時間と退室時間を取得
        entry_time_str = attendance.get('entry1', {}).get('read_datetime')
        exit_time_str = attendance.get('exit1', {}).get('read_datetime', None)
        if not entry_time_str:
            print("入室時間が見つかりません。スキップします。")
            return False

        print(f"入室時間: {entry_time_str}")
        print(f"退室時間: {exit_time_str if exit_time_str else '退室時間なし'}")

        # 入室時間を日付オブジェクトに変換
        entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")

        # コースの開始・終了時間を取得
        schedule = course.get('schedule', {}).get('time', '')
        if not schedule or '~' not in schedule:
            print("スケジュール情報が不正です。スキップします。")
            return False
        start_time_str, end_time_str = schedule.split('~')
        start_time = datetime.datetime.strptime(start_time_str, "%H:%M")
        end_time = datetime.datetime.strptime(end_time_str, "%H:%M")

        print(f"コース開始時間: {start_time_str}")
        print(f"コース終了時間: {end_time_str}")

        # 退室時間がある場合は変換、ない場合は終了時間を使用
        if exit_time_str:
            exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
        else:
            exit_time = end_time

        print(f"実際の退室時間: {exit_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # 終了時間1＋15分を計算
        end_time_with_buffer = end_time + datetime.timedelta(minutes=15)
        print(f"終了時間1＋15分: {end_time_with_buffer.strftime('%H:%M')}")

        # 退室時間1が終了時間1＋15分以降の場合
        if exit_time > end_time_with_buffer:
            print(f"退室時間1が終了時間1＋15分以降です。処理を実行します。")

            # 退室時間1を一時保存
            original_exit_time = exit_time
            print(f"元の退室時間1: {original_exit_time.strftime('%Y-%m-%d %H:%M:%S')} を一時保存します。")

            # 退室時間1を終了時間1に設定
            exit_time = end_time
            print(f"退室時間1を終了時間1に設定: {exit_time.strftime('%Y-%m-%d %H:%M:%S')}")

            # 入室時間2を終了時間1＋10分に設定
            entry_time2 = end_time + datetime.timedelta(minutes=10)
            print(f"新しい入室時間2を設定: {entry_time2.strftime('%Y-%m-%d %H:%M:%S')}")

            # Firebaseにデータを保存
            student_id = attendance.get('student_id')
            if student_id:
                print(f"Firebaseにデータを保存します（学生ID: {student_id}）")
                ref = db.reference(f'Students/attendance/students_id/{student_id}')
                ref.update({
                    'exit1': {
                        'original_read_datetime': original_exit_time.strftime("%Y-%m-%d %H:%M:%S")
                    },
                    'entry2': {
                        'read_datetime': entry_time2.strftime("%Y-%m-%d %H:%M:%S"),
                        'status': '正常出席'
                    }
                })
                print("Firebaseへの保存が完了しました。")

            # コースID2へ移行
            print(f"コースID {course_id} からコースID 2 へ移行します。")
            return True

        print("退室時間は正常範囲内のため、特別な処理は不要です。")
        return False
    except Exception as e:
        print(f"出席処理中にエラーが発生しました: {e}")
        return False

# 出席を記録する関数
def record_attendance(students_data, courses_data):
    print("出席記録を開始します...")
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    courses_list = courses_data.get('course_id', [])
    print(f"取得した出席データの数: {len(attendance_data)}")

    for student_id, attendance in attendance_data.items():
        print(f"学生ID: {student_id} の出席を確認します...")
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。スキップします。")
            continue

        course_ids = attendance.get('course_id', [])
        print(f"学生 {student_id} の登録コースID: {course_ids}")

        for course_id in course_ids:
            try:
                course_id_int = int(course_id)
                course = courses_list[course_id_int]
                if not course:
                    raise ValueError(f"コースID {course_id} に対応する授業が見つかりません。")
            except (ValueError, IndexError):
                print(f"無効なコースID {course_id} が見つかりました。スキップします。")
                continue

            if check_and_handle_attendance(attendance, course, course_id):
                print(f"学生 {student_id} のコースID {course_id} の出席処理が完了しました。")

    print("すべての出席記録処理が完了しました。")

# Firebaseからデータを取得し、出席を記録
print("Firebaseから学生データを取得します...")
students_data = get_data_from_firebase('Students')
print("Firebaseからコースデータを取得します...")
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
