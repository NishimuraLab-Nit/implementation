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

# 退室・入室時間のチェックと更新処理
def process_exit_and_entry(attendance, course, course_id, student_id):
    try:
        # 終了時間1を取得
        _, end_time_str = course.get('schedule', {}).get('time', '').split('~')
        end_time = datetime.datetime.strptime(end_time_str, "%H:%M")
        
        # 退室時間1を取得
        exit1_str = attendance.get('exit1', {}).get('read_datetime')
        if exit1_str:
            exit1_time = datetime.datetime.strptime(exit1_str, "%Y-%m-%d %H:%M:%S")
        else:
            # 退室時間1が存在しない場合、終了時間1を設定
            exit1_time = end_time
            attendance['exit1'] = {'read_datetime': exit1_time.strftime("%Y-%m-%d %H:%M:%S")}
            print(f"退室時間1が存在しないため、終了時間1を設定しました（学生ID: {student_id}）")

        # 終了時間1 + 15分を計算
        end_time_plus_15 = end_time + datetime.timedelta(minutes=15)

        # 条件1: 退室時間1が「終了時間1 + 15分以降」
        if exit1_time >= end_time_plus_15:
            # 退室時間1を終了時間1に設定
            attendance['exit1']['read_datetime'] = end_time.strftime("%Y-%m-%d %H:%M:%S")
            # 入室時間2を「終了時間1 + 10分」に設定
            entry2_time = end_time + datetime.timedelta(minutes=10)
            attendance['entry2'] = {'read_datetime': entry2_time.strftime("%Y-%m-%d %H:%M:%S")}
            print(f"退室時間1が終了時間1 + 15分以降だったため、退室時間1と入室時間2を更新しました（学生ID: {student_id}）")

        # Firebaseに保存
        ref = db.reference(f"Students/attendance/students_id/{student_id}")
        ref.update(attendance)

        # コース1を正常出席と判定
        print(f"コース1を正常出席と判定しました（学生ID: {student_id}）")
        return True

    except Exception as e:
        print(f"退室・入室時間処理中にエラーが発生しました（学生ID: {student_id}）: {e}")
        return False

# 出席を記録する関数
def record_attendance(students_data, courses_data):
    # 各データを取得
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    courses_list = courses_data.get('course_id', [])

    # 各学生の出席を確認
    for student_id, attendance in attendance_data.items():
        # 学生のコース情報を取得
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。")
            continue

        course_ids = student_info.get('classes_taken', '').split(', ')
        for course_id_str in course_ids:
            try:
                course_id = int(course_id_str)
                course = courses_list[course_id]
                if not course:
                    raise ValueError(f"コースID {course_id} に対応する授業が見つかりません。")
            except (ValueError, IndexError):
                print(f"無効なコースID {course_id_str} が見つかりました。スキップします。")
                continue

            # 条件を満たす退室・入室時間の処理
            if not process_exit_and_entry(attendance, course, course_id, student_id):
                continue

            # 「終了時間2 + 5分以降」の処理（コース2へ移行）
            if course_id + 1 < len(courses_list):
                next_course = courses_list[course_id + 1]
                _, next_end_time_str = next_course.get('schedule', {}).get('time', '').split('~')
                next_end_time = datetime.datetime.strptime(next_end_time_str, "%H:%M")
                next_end_time_plus_5 = next_end_time + datetime.timedelta(minutes=5)

                exit1_str = attendance.get('exit1', {}).get('read_datetime')
                if exit1_str:
                    exit1_time = datetime.datetime.strptime(exit1_str, "%Y-%m-%d %H:%M:%S")
                    if exit1_time >= next_end_time_plus_5:
                        print(f"コース2も正常出席と判定しました（学生ID: {student_id}）")

# Firebaseからデータを取得し、出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
