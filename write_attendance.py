import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseアプリの初期化（未初期化の場合のみ実行）
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Google Sheets API用のスコープと認証
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
client = gspread.authorize(creds)

# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    ref = db.reference(path)
    return ref.get()

# 時間を比較する関数
def compare_times(entry, exit, start, finish):
    entry_time = datetime.datetime.strptime(entry, "%Y-%m-%d %H:%M:%S")
    exit_time = datetime.datetime.strptime(exit, "%Y-%m-%d %H:%M:%S")
    start_time = datetime.datetime.strptime(start, "%H%M")
    finish_time = datetime.datetime.strptime(finish, "%H%M")

    # 欠席（✕）
    if entry_time > finish_time:
        return "✕ 欠席"

    # 出席（〇）
    if entry_time <= start_time + datetime.timedelta(minutes=5) and exit_time <= finish_time + datetime.timedelta(minutes=5):
        return "〇 出席"

    # 遅刻（△遅）
    if entry_time > start_time + datetime.timedelta(minutes=5) and exit_time <= finish_time + datetime.timedelta(minutes=5):
        late_minutes = (entry_time - start_time).seconds // 60
        return f"△ 遅刻 {late_minutes}分"

    # 早退（△早）
    if entry_time <= start_time + datetime.timedelta(minutes=5) and exit_time < finish_time - datetime.timedelta(minutes=5):
        early_minutes = (finish_time - exit_time).seconds // 60
        return f"△ 早退 {early_minutes}分"

    # 出席遅延調整
    if exit_time > finish_time + datetime.timedelta(minutes=5):
        new_exit = finish_time
        new_entry = finish_time + datetime.timedelta(minutes=10)
        return "〇 出席（調整済み）", new_exit, new_entry

    return "不明"

# Google Sheetsにデータを書き込む関数
def write_to_sheet(sheet_id, sheet_name, column, row, value):
    try:
        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
        sheet.update_cell(row, column, value)
        print(f"シート {sheet_name} にデータを書き込みました: {value}")
    except Exception as e:
        print(f"Google Sheets 書き込み中にエラーが発生しました: {e}")

# メイン処理
def main():
    # Firebaseから学生データを取得
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    student_info = get_data_from_firebase("Students/student_info/student_index")
    enrollment_data = get_data_from_firebase("Students/enrollment/student_index")
    courses_data = get_data_from_firebase("Courses/course_id")

    for student_id, attendance in attendance_data.items():
        student_index = student_info.get(student_id, {}).get("student_index")
        if not student_index:
            continue

        # 各学生のコースを取得
        student_courses = enrollment_data.get(student_index, {}).get("course_id", "").split(", ")
        for course_id in student_courses:
            if not course_id.isdigit():
                continue
            course_id = int(course_id)
            course_data = courses_data[course_id]
            schedule = course_data["schedule"]
            start_time, finish_time = schedule["time"].split("~")

            for entry_key, entry_data in attendance.items():
                if not entry_key.startswith("entry"):
                    continue

                entry_time = entry_data["read_datetime"]
                exit_key = "exit" + entry_key[-1]
                exit_time = attendance.get(exit_key, {}).get("read_datetime", None)

                # 時間を比較して結果を取得
                if exit_time:
                    result = compare_times(entry_time, exit_time, start_time, finish_time)
                else:
                    result = "✕ 欠席"

                # Google Sheetsに書き込み
                sheet_id = student_info[student_index]["sheet_id"]
                sheet_name = datetime.datetime.now().strftime("%Y-%m")
                column = int(entry_time.split("-")[2]) + 1  # 日付
                row = course_id + 1  # コースの個数目
                write_to_sheet(sheet_id, sheet_name, column, row, result)

if __name__ == "__main__":
    main()
