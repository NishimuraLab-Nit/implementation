import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebase & GSpread初期化
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

scope = ["https://spreadsheets.google.com/feeds", 
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gclient = gspread.authorize(creds)

# Firebaseからデータ取得
def get_data_from_firebase(path):
    print(f"Firebaseからデータを取得: {path}")
    ref = db.reference(path)
    data = ref.get()
    print(f"取得したデータ: {data}")
    return data

# 時間のパース
def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    try:
        return datetime.datetime.strptime(dt_str, fmt)
    except (ValueError, TypeError):
        print(f"日時のパースに失敗: {dt_str}")
        return None

def parse_hhmm_range(range_str):
    try:
        start_str, end_str = range_str.split("~")
        sh, sm = map(int, start_str.split(":"))
        eh, em = map(int, end_str.split(":"))
        return datetime.time(sh, sm), datetime.time(eh, em)
    except:
        print(f"時間範囲のパースに失敗: {range_str}")
        return None, None

def combine_date_and_time(date, time):
    return datetime.datetime(date.year, date.month, date.day, time.hour, time.minute)

# 出席判定ロジック
def judge_attendance(entry_dt, exit_dt, start_dt, finish_dt):
    print(f"出席判定中: entry={entry_dt}, exit={exit_dt}, start={start_dt}, finish={finish_dt}")
    td_5min = datetime.timedelta(minutes=5)

    if entry_dt >= finish_dt:
        print("判定結果: ×")
        return "×"
    if entry_dt <= (start_dt + td_5min) and (exit_dt is None or exit_dt >= finish_dt):
        print("判定結果: ○")
        return "○"
    if entry_dt > (start_dt + td_5min):
        print("判定結果: △遅")
        return "△遅"
    print("判定結果: △早")
    return "△早"

# 出席データの処理とシートへの書き込み
def process_attendance_and_write_sheet():
    print("出席データ処理を開始します。")
    courses = get_data_from_firebase("Courses")
    if not courses:
        print("Coursesデータが存在しません。")
        return

    # Coursesデータがリストまたは辞書かを判定
    if isinstance(courses, list):
        course_list = [course for course in courses if isinstance(course, dict)]
    elif isinstance(courses, dict):
        course_list = list(courses.values())
    else:
        print("無効なCoursesデータ形式です。")
        return

    for course_data in course_list:
        if not isinstance(course_data, dict):
            print(f"無効なコースデータ: {course_data}")
            continue

        print(f"コースデータ: {course_data}")
        schedule = course_data.get("schedule", {})
        course_sheet_id = course_data.get("course_sheet_id")
        if not course_sheet_id:
            print("シートIDが見つかりません。")
            continue

        try:
            sheet = gclient.open_by_key(course_sheet_id)
            print(f"シート接続成功: {course_sheet_id}")
        except Exception as e:
            print(f"シート接続エラー: {e}")
            continue

        enrollments = get_data_from_firebase(f"Students/enrollment/course_id/{course_data.get('serial_number')}")
        if not enrollments:
            print("登録データが見つかりません。")
            continue

        for student_index, enrollment in enrollments.items():
            print(f"受講生インデックス: {student_index}, 登録データ: {enrollment}")
            student_id = get_data_from_firebase(f"Students/student_info/student_index/{student_index}/student_id")
            if not student_id:
                print(f"受講生インデックス {student_index} に対応する学生IDが見つかりません。")
                continue

            attendance = get_data_from_firebase(f"Students/attendance/student_id/{student_id}")
            if not attendance:
                print(f"学生ID {student_id} の出席データが見つかりません。")
                continue

            for entry_key, entry_data in attendance.items():
                entry_dt = parse_datetime(entry_data.get("read_datetime"))
                if not entry_dt:
                    print(f"無効なエントリーデータ: {entry_data}")
                    continue

                start_time, end_time = parse_hhmm_range(schedule.get("time"))
                if not start_time or not end_time:
                    print(f"無効なスケジュール時間: {schedule.get('time')}")
                    continue

                start_dt = combine_date_and_time(entry_dt.date(), start_time)
                end_dt = combine_date_and_time(entry_dt.date(), end_time)
                status = judge_attendance(entry_dt, None, start_dt, end_dt)

                yyyymm = entry_dt.strftime("%Y-%m")
                day = entry_dt.day

                try:
                    worksheet = sheet.worksheet(yyyymm)
                except gspread.exceptions.WorksheetNotFound:
                    print(f"シート {yyyymm} が見つかりません。新規作成します。")
                    worksheet = sheet.add_worksheet(title=yyyymm, rows=50, cols=50)

                row = int(student_index) + 1
                col = day + 1
                print(f"シートに書き込み: 行={row}, 列={col}, 値={status}")
                worksheet.update_cell(row, col, status)

    print("出席処理が完了しました。")

if __name__ == "__main__":
    process_attendance_and_write_sheet()
