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
    print("Firebaseアプリ初期化完了。")

# Google Sheets API用のスコープを設定
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
client = gspread.authorize(creds)
print("Google Sheets API 認証完了。")

# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    print(f"Firebaseからデータを取得中: {path}")
    ref = db.reference(path)
    data = ref.get()
    if data is None:
        print(f"Firebaseからデータが見つかりません: {path}")
    else:
        print(f"Firebaseからデータを取得しました: {path}")
    return data

# 出席判定ロジック
def determine_attendance_status(entry_time, exit_time, start_time, end_time):
    print(f"出席判定中: entry_time={entry_time}, exit_time={exit_time}, start_time={start_time}, end_time={end_time}")
    start_plus_5 = start_time + datetime.timedelta(minutes=5)
    end_minus_5 = end_time - datetime.timedelta(minutes=5)
    end_plus_15 = end_time + datetime.timedelta(minutes=15)

    print(f"基準時間: start_plus_5={start_plus_5}, end_minus_5={end_minus_5}, end_plus_15={end_plus_15}")

    if entry_time <= start_plus_5:
        if exit_time is None or exit_time >= end_minus_5:
            print("判定結果: 正常出席 (○)")
            return "○"  # 正常出席
        elif exit_time < end_minus_5:
            early_minutes = (end_minus_5 - exit_time).seconds // 60
            print(f"判定結果: 早退 (△早{early_minutes}分)")
            return f"△早{early_minutes}分"  # 早退
    elif entry_time > start_plus_5:
        if exit_time is None or exit_time >= end_minus_5:
            late_minutes = (entry_time - start_plus_5).seconds // 60
            print(f"判定結果: 遅刻 (△遅{late_minutes}分)")
            return f"△遅{late_minutes}分"  # 遅刻
    if entry_time > end_time:
        print("判定結果: 欠席 (×)")
        return "×"  # 欠席
    if entry_time <= start_plus_5 and exit_time >= end_plus_15:
        print("判定結果: 同教室 (○ 同教室)")
        return "○ 同教室"  # 同教室判定
    print("判定結果: 欠席 (×)")
    return "×"  # デフォルトは欠席

# 出席を確認しマークする関数
def check_and_mark_attendance(attendance, course, sheet, entry_label, course_id):
    print(f"出席確認中: 学生の出席データ={attendance}, コースデータ={course}, entry_label={entry_label}")

    entry_time_str = attendance.get(entry_label, {}).get('read_datetime')
    exit_time_str = attendance.get(entry_label, {}).get('exit_datetime')

    if not entry_time_str:
        print(f"入室時間が存在しません: entry_label={entry_label}")
        return False

    entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
    exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S") if exit_time_str else None

    print(f"入室時間: {entry_time}, 退室時間: {exit_time}")

    schedule = course.get('schedule', {}).get('time', '')
    if not schedule:
        print(f"スケジュールデータが見つかりません: コース={course}")
        return False

    start_time_str, end_time_str = schedule.split('~')
    start_time = datetime.datetime.strptime(start_time_str, "%H:%M")
    end_time = datetime.datetime.strptime(end_time_str, "%H:%M")

    print(f"スケジュール: 開始時間={start_time}, 終了時間={end_time}")

    status = determine_attendance_status(entry_time, exit_time, start_time, end_time)

    entry_month = entry_time.strftime("%Y-%m")
    try:
        sheet_to_update = sheet.worksheet(entry_month)
    except gspread.exceptions.WorksheetNotFound:
        print(f"シート '{entry_month}' が見つかりません。スキップします。")
        return False

    row = int(course_id) + 1
    column = entry_time.day + 1
    print(f"シート更新: row={row}, column={column}, status={status}")
    sheet_to_update.update_cell(row, column, status)
    print(f"出席記録完了: {course['class_name']} - {entry_label} - ステータス: {status}")
    return True

# 出席を記録する関数
def record_attendance(students_data, courses_data):
    print("出席記録処理を開始します...")
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    student_index_data = students_data.get('student_info', {}).get('student_index', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        print(f"学生ID: {student_id}")
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。")
            continue

        student_index = student_info.get('student_index')
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', [])

        sheet_id = student_index_data.get(student_index, {}).get('sheet_id')
        if not sheet_id:
            print(f"学生インデックス {student_index} に対応するスプレッドシートIDが見つかりません。")
            continue

        try:
            sheet = client.open_by_key(sheet_id)
            print(f"スプレッドシートを開きました: {sheet_id}")
        except Exception as e:
            print(f"スプレッドシート {sheet_id} を開けません: {e}")
            continue

        for course_id in course_ids:
            print(f"コースID: {course_id}")
            try:
                course_id_int = int(course_id)
                course = courses_list[course_id_int]
                if not course:
                    raise ValueError(f"コースID {course_id} に対応する授業が見つかりません。")
            except (ValueError, IndexError):
                print(f"無効なコースID {course_id} が見つかりました。スキップします。")
                continue

            if check_and_mark_attendance(attendance, course, sheet, 'entry1', course_id):
                continue

            if 'entry2' in attendance:
                check_and_mark_attendance(attendance, course, sheet, 'entry2', course_id)

# メイン処理
print("Firebaseからデータを取得します...")
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
print("Firebaseからのデータ取得完了。")

record_attendance(students_data, courses_data)
print("出席記録処理が完了しました。")
