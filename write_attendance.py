import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseアプリの初期化（未初期化の場合のみ実行）
if not firebase_admin._apps:
    print("Firebaseアプリを初期化中...")
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })
    print("Firebaseアプリが初期化されました。")

# Google Sheets API用のスコープと認証
print("Google Sheets APIの認証情報を読み込み中...")
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
        print(f"取得成功: {len(data)}件のデータを取得しました。")
    else:
        print("データが存在しません。")
    return data

# 時間を比較する関数
def compare_times(entry, exit, start, finish):
    print(f"時間比較開始: entry={entry}, exit={exit}, start={start}, finish={finish}")
    entry_time = datetime.datetime.strptime(entry, "%Y-%m-%d %H:%M:%S")
    exit_time = datetime.datetime.strptime(exit, "%Y-%m-%d %H:%M:%S")
    start_time = datetime.datetime.strptime(start, "%H%M")
    finish_time = datetime.datetime.strptime(finish, "%H%M")

    # 欠席（✕）
    if entry_time > finish_time:
        print("判定結果: 欠席 (✕)")
        return "✕ 欠席"

    # 出席（〇）
    if entry_time <= start_time + datetime.timedelta(minutes=5) and exit_time <= finish_time + datetime.timedelta(minutes=5):
        print("判定結果: 出席 (〇)")
        return "〇 出席"

    # 遅刻（△遅）
    if entry_time > start_time + datetime.timedelta(minutes=5) and exit_time <= finish_time + datetime.timedelta(minutes=5):
        late_minutes = (entry_time - start_time).seconds // 60
        print(f"判定結果: 遅刻 (△遅) {late_minutes}分")
        return f"△ 遅刻 {late_minutes}分"

    # 早退（△早）
    if entry_time <= start_time + datetime.timedelta(minutes=5) and exit_time < finish_time - datetime.timedelta(minutes=5):
        early_minutes = (finish_time - exit_time).seconds // 60
        print(f"判定結果: 早退 (△早) {early_minutes}分")
        return f"△ 早退 {early_minutes}分"

    # 出席遅延調整
    if exit_time > finish_time + datetime.timedelta(minutes=5):
        print("判定結果: 出席調整が必要")
        new_exit = finish_time
        new_entry = finish_time + datetime.timedelta(minutes=10)
        print(f"調整後: new_exit={new_exit}, new_entry={new_entry}")
        return "〇 出席（調整済み）", new_exit, new_entry

    print("判定結果: 不明")
    return "不明"

# Google Sheetsにデータを書き込む関数
def write_to_sheet(sheet_id, sheet_name, column, row, value):
    print(f"Google Sheetsにデータを書き込み中: sheet_id={sheet_id}, sheet_name={sheet_name}, column={column}, row={row}, value={value}")
    try:
        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
        sheet.update_cell(row, column, value)
        print(f"シート {sheet_name} のセル({row}, {column}) にデータを書き込みました: {value}")
    except Exception as e:
        print(f"Google Sheets 書き込み中にエラーが発生しました: {e}")

# メイン処理
def main():
    print("メイン処理を開始します。")

    # Firebaseから必要なデータを取得
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    student_info = get_data_from_firebase("Students/student_info/student_index")
    enrollment_data = get_data_from_firebase("Students/enrollment/student_index")
    courses_data = get_data_from_firebase("Courses/course_id")

    # 学生の出席データをループ
    for student_id, attendance in attendance_data.items():
        print(f"処理中の学生ID: {student_id}")
        student_index = student_info.get(student_id, {}).get("student_index")
        if not student_index:
            print(f"学生ID {student_id} に対応する student_index が見つかりません。スキップします。")
            continue

        # 各学生のコースを取得
        student_courses = enrollment_data.get(student_index, {}).get("course_id", "").split(", ")
        print(f"学生 {student_index} のコース: {student_courses}")

        for course_id in student_courses:
            if not course_id.isdigit():
                print(f"無効なコースID: {course_id}。スキップします。")
                continue
            course_id = int(course_id)
            course_data = courses_data[course_id]
            schedule = course_data["schedule"]
            start_time, finish_time = schedule["time"].split("~")

            print(f"コース {course_id} のスケジュール: start={start_time}, finish={finish_time}")

            for entry_key, entry_data in attendance.items():
                if not entry_key.startswith("entry"):
                    continue

                entry_time = entry_data["read_datetime"]
                exit_key = "exit" + entry_key[-1]
                exit_time = attendance.get(exit_key, {}).get("read_datetime", None)

                print(f"entry_time={entry_time}, exit_time={exit_time}")

                # 時間を比較して結果を取得
                if exit_time:
                    result = compare_times(entry_time, exit_time, start_time, finish_time)
                else:
                    print("exit_time が存在しないため、欠席と判定します。")
                    result = "✕ 欠席"

                # Google Sheetsに書き込み
                sheet_id = student_info[student_index]["sheet_id"]
                sheet_name = datetime.datetime.now().strftime("%Y-%m")
                column = int(entry_time.split("-")[2]) + 1  # 日付
                row = course_id + 1  # コースの個数目
                write_to_sheet(sheet_id, sheet_name, column, row, result)

    print("メイン処理が完了しました。")

if __name__ == "__main__":
    main()
