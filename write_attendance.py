import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseアプリの初期化（未初期化の場合のみ実行）
if not firebase_admin._apps:
    print("Firebaseアプリを初期化しています...")
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Google Sheets API用のスコープを設定
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

print("Google Sheets APIを認証しています...")
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
client = gspread.authorize(creds)

# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    print(f"Firebaseからデータを取得しています: パス={path}")
    ref = db.reference(path)
    data = ref.get()
    print(f"{path}から取得したデータ: {data}")
    return data

# 時刻フォーマットの変換
def parse_time(time_str, fmt="%H%M~%H%M"):
    print(f"時刻文字列を解析しています: {time_str}")
    start, end = time_str.split("~")
    start_time = datetime.datetime.strptime(start, "%H%M")
    end_time = datetime.datetime.strptime(end, "%H%M")
    print(f"解析結果 - 開始時刻: {start_time}, 終了時刻: {end_time}")
    return start_time, end_time

# 日時フォーマットの変換
def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    print(f"日時文字列を解析しています: {dt_str}")
    dt = datetime.datetime.strptime(dt_str, fmt)
    print(f"解析結果 - 日時: {dt}")
    return dt

# メイン処理
def main():
    print("メイン処理を開始します...")
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    student_info_data = get_data_from_firebase("Students/student_info/student_index")
    enrollment_data = get_data_from_firebase("Students/enrollment/student_index")
    courses_data = get_data_from_firebase("Courses")

    for student_id, attendance in attendance_data.items():
        print(f"学生ID {student_id} の出席データを処理しています...")
        student_index = get_data_from_firebase(f"Students/student_info/student_id/{student_id}/student_index")
        if not student_index:
            print(f"学生ID {student_id} に対応する学生インデックスが見つかりませんでした。")
            continue

        course_ids = enrollment_data.get(student_index, {}).get("course_id", "").split(", ")
        print(f"学生ID {student_id} が登録しているコース: {course_ids}")
        
        for course_id in course_ids:
            if not course_id:
                print(f"学生ID {student_id} に有効なコースIDが見つかりませんでした。")
                continue

            course_data = next((course for course in courses_data if course.get("id") == int(course_id)), None)
            if not course_data:
                print(f"コースID {course_id} に該当するデータが見つかりませんでした。")
                continue

            schedule = course_data.get("schedule", {}).get("time", "")
            if not schedule:
                print(f"コースID {course_id} にスケジュールが見つかりませんでした。")
                continue

            start_time, end_time = parse_time(schedule)

            for entry_key, entry_data in attendance.items():
                print(f"エントリーキー {entry_key} を処理しています（学生ID: {student_id}）...")
                if not entry_key.startswith("entry"):
                    print(f"エントリーキーではないためスキップします: {entry_key}")
                    continue

                entry_time = parse_datetime(entry_data["read_datetime"])
                exit_key = entry_key.replace("entry", "exit")
                exit_time = parse_datetime(attendance.get(exit_key, {}).get("read_datetime", start_time.strftime("%Y-%m-%d %H:%M:%S")))

                if entry_time > exit_time:
                    status = "✕"
                elif entry_time <= start_time + datetime.timedelta(minutes=5) and exit_time <= end_time + datetime.timedelta(minutes=5):
                    status = "〇"
                elif entry_time > start_time + datetime.timedelta(minutes=5) and exit_time <= end_time + datetime.timedelta(minutes=5):
                    late_minutes = (entry_time - start_time).seconds // 60
                    status = f"△遅{late_minutes}分"
                elif entry_time <= start_time + datetime.timedelta(minutes=5) and exit_time < end_time - datetime.timedelta(minutes=5):
                    early_leave_minutes = (end_time - exit_time).seconds // 60
                    status = f"△早{early_leave_minutes}分"
                else:
                    status = "✕"

                print(f"エントリー時刻: {entry_time}, 退出時刻: {exit_time}, 判定ステータス: {status}")

                if status in ["〇", "△遅", "△早"]:
                    if exit_time > end_time + datetime.timedelta(minutes=5):
                        new_exit_time = end_time
                        new_entry_time = end_time + datetime.timedelta(minutes=10)
                        attendance[exit_key] = {"read_datetime": new_exit_time.strftime("%Y-%m-%d %H:%M:%S")}
                        attendance[f"entry{int(entry_key[-1]) + 1}"] = {"read_datetime": new_entry_time.strftime("%Y-%m-%d %H:%M:%S")}
                        ref = db.reference(f"Students/attendance/student_id/{student_id}")
                        ref.update(attendance)

            sheet_id = student_info_data.get(student_index, {}).get("sheet_id", "")
            if sheet_id:
                print(f"Googleシートを更新しています（学生ID: {student_id}, シートID: {sheet_id}）...")
                sheet = client.open_by_key(sheet_id).worksheet(datetime.datetime.now().strftime("%Y-%m"))
                day_column = entry_time.day + 1
                course_row = int(course_id) + 1
                sheet.update_cell(course_row, day_column, status)
                print(f"セルを更新しました: 行 {course_row}, 列 {day_column}, ステータス: {status}")

if __name__ == "__main__":
    main()
