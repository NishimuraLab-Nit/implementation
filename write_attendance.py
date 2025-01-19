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

# Firebaseへデータを書き込む関数
def save_to_firebase(path, data):
    print(f"Firebaseの'{path}'にデータを書き込みます: {data}")
    ref = db.reference(path)
    ref.update(data)
    print("書き込み完了。")

# 出席を記録する関数
def record_attendance_with_additional_logic(attendance_data, courses_data):
    print("\n出席記録の出席評価ロジックを実行します。")
    for student_id, attendance in attendance_data.items():
        print(f"\n学生ID: {student_id}")
        for entry_index, course_id in enumerate(attendance.get('courses', []), start=1):
            print(f"学生 {student_id} のコースID {course_id} の出席を評価します。")

            course = courses_data.get(course_id, {})
            if not course:
                print(f"コースID {course_id} のデータが見つかりません。")
                continue

            end_time_str = course.get('schedule', {}).get('end_time', None)
            if not end_time_str:
                print(f"終了時間が指定されていません。")
                continue

            end_minutes = time_to_minutes(end_time_str)
            entry_key = f'entry{entry_index}'
            exit_key = f'exit{entry_index}'
            exit_time_str = attendance.get(exit_key, {}).get('read_datetime', None)

            if exit_time_str:
                exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
                exit_minutes = exit_time.hour * 60 + exit_time.minute

                # 終了時間1+5分以降
                if exit_minutes >= end_minutes + 5:
                    save_to_firebase(f"attendance/{student_id}/{entry_key}/read_datetime", {"read_datetime": end_time_str})
                    print(f"学生 {student_id} の出席は正常出席と判断されました。")

            else:
                # 退室時間が存在しない場合
                save_to_firebase(f"attendance/{student_id}/{exit_key}/read_datetime", {"read_datetime": end_time_str})
                next_entry_time = (datetime.datetime.strptime(end_time_str, "%H:%M") + datetime.timedelta(minutes=10)).strftime("%H:%M")
                save_to_firebase(f"attendance/{student_id}/entry{entry_index + 1}/read_datetime", {"read_datetime": next_entry_time})
                print(f"退室時間がありませんでしたがデータを書き込みました。")

# Firebaseからデータを取得し、出席記録を書き込む
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance_with_additional_logic(students_data, courses_data)
