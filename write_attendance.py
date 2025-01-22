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

# 時刻フォーマットの変換
def parse_time(time_str, fmt="%H%M~%H%M"):
    start, end = time_str.split("~")
    start_time = datetime.datetime.strptime(start, "%H%M")
    end_time = datetime.datetime.strptime(end, "%H%M")
    return start_time, end_time

# 日時フォーマットの変換
def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.datetime.strptime(dt_str, fmt)

# メイン処理
def main():
    # Firebaseから必要なデータを取得
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    student_info_data = get_data_from_firebase("Students/student_info/student_index")
    enrollment_data = get_data_from_firebase("Students/enrollment/student_index")
    courses_data = get_data_from_firebase("Courses")

    # 各学生の出席記録を処理
    for student_id, attendance in attendance_data.items():
        student_index = get_data_from_firebase(f"Students/student_info/student_id/{student_id}/student_index")
        if not student_index:
            continue

        # 学生が登録しているコースを取得
        course_ids = enrollment_data.get(student_index, {}).get("course_id", "").split(", ")
        for course_id in course_ids:
            if not course_id:
                continue

            # コースのスケジュールを取得
            schedule = courses_data.get(int(course_id), {}).get("schedule", {}).get("time", "")
            if not schedule:
                continue

            # スケジュールの開始・終了時刻を解析
            start_time, end_time = parse_time(schedule)

            # 出席記録を解析
            for entry_key, entry_data in attendance.items():
                if not entry_key.startswith("entry"):
                    continue
                entry_time = parse_datetime(entry_data["read_datetime"])
                exit_key = entry_key.replace("entry", "exit")
                exit_time = parse_datetime(attendance.get(exit_key, {}).get("read_datetime", start_time.strftime("%Y-%m-%d %H:%M:%S")))

                # 欠席、出席、遅刻、早退の判定
                if entry_time > exit_time:
                    status = "✕"  # 欠席
                elif entry_time <= start_time + datetime.timedelta(minutes=5) and exit_time <= end_time + datetime.timedelta(minutes=5):
                    status = "〇"  # 出席
                elif entry_time > start_time + datetime.timedelta(minutes=5) and exit_time <= end_time + datetime.timedelta(minutes=5):
                    late_minutes = (entry_time - start_time).seconds // 60
                    status = f"△遅{late_minutes}分"  # 遅刻
                elif entry_time <= start_time + datetime.timedelta(minutes=5) and exit_time < end_time - datetime.timedelta(minutes=5):
                    early_leave_minutes = (end_time - exit_time).seconds // 60
                    status = f"△早{early_leave_minutes}分"  # 早退
                else:
                    status = "✕"  # その他は欠席とみなす

                # Firebaseに保存する（必要に応じて更新）
                if status in ["〇", "△遅", "△早"]:
                    # exit1やentry2を調整
                    if exit_time > end_time + datetime.timedelta(minutes=5):
                        new_exit_time = end_time
                        new_entry_time = end_time + datetime.timedelta(minutes=10)
                        attendance[exit_key] = {"read_datetime": new_exit_time.strftime("%Y-%m-%d %H:%M:%S")}
                        attendance[f"entry{int(entry_key[-1]) + 1}"] = {"read_datetime": new_entry_time.strftime("%Y-%m-%d %H:%M:%S")}
                        ref = db.reference(f"Students/attendance/student_id/{student_id}")
                        ref.update(attendance)

            # Google Sheetsに記録
            sheet_id = student_info_data.get(student_index, {}).get("sheet_id", "")
            if sheet_id:
                sheet = client.open_by_key(sheet_id).worksheet(datetime.datetime.now().strftime("%Y-%m"))
                day_column = entry_time.day + 1
                course_row = int(course_id) + 1
                sheet.update_cell(course_row, day_column, status)

if __name__ == "__main__":
    main()
