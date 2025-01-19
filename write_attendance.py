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

# Google Sheets API用のスコープを設定
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
client = gspread.authorize(creds)

# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    ref = db.reference(path)
    return ref.get()

# 出席判定関数
def determine_attendance(entry_time, exit_time, start_time, end_time):
    """
    入室時間と退室時間を基に出席状況を判定する。
    """
    result = None

    # 時間を分単位に変換
    entry_minutes = entry_time.hour * 60 + entry_time.minute
    exit_minutes = exit_time.hour * 60 + exit_time.minute
    start_minutes = start_time.hour * 60 + start_time.minute
    end_minutes = end_time.hour * 60 + end_time.minute

    # 出席判定ロジック
    if entry_minutes <= start_minutes + 5:  # 入室が「開始時間＋5分以内」
        if exit_minutes >= end_minutes - 5:  # 退室が「終了時間−5分以降」
            result = "○"  # 正常出席
        elif exit_minutes < end_minutes - 5:  # 退室が早い場合
            early_minutes = end_minutes - 5 - exit_minutes
            result = f"△早{early_minutes}分"  # 早退
    elif entry_minutes > start_minutes + 5:  # 遅刻の場合
        if exit_minutes >= end_minutes - 5:  # 退室が「終了時間−5分以降」
            late_minutes = entry_minutes - (start_minutes + 5)
            result = f"△遅{late_minutes}分"  # 遅刻
    elif entry_minutes >= end_minutes:  # 入室が「終了時間」以降
        result = "×"  # 欠席

    return result

# 出席を記録する関数
def record_attendance_for_course(attendance, course, sheet, entry_label, exit_label, course_id):
    """
    指定されたコースIDに基づき、出席を記録する。
    """
    # 入室時間・退室時間を取得
    entry_time_str = attendance.get(entry_label, {}).get('read_datetime')
    exit_time_str = attendance.get(exit_label, {}).get('read_datetime', None)

    if not entry_time_str:
        print(f"{entry_label} のデータが見つかりません。")
        return False

    # 時間を日付オブジェクトに変換
    entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
    start_time_str, end_time_str = course.get('schedule', {}).get('time', '').split('~')
    start_time = datetime.datetime.strptime(start_time_str, "%H:%M")
    end_time = datetime.datetime.strptime(end_time_str, "%H:%M")
    exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S") if exit_time_str else end_time

    # 出席状況を判定
    result = determine_attendance(entry_time, exit_time, start_time, end_time)
    if not result:
        print(f"出席状況を判定できませんでした: {entry_label}")
        return False

    # スプレッドシートに記録
    try:
        entry_month = entry_time.strftime("%Y-%m")  # シート名に使用する年月
        sheet_to_update = sheet.worksheet(entry_month)
    except gspread.exceptions.WorksheetNotFound:
        print(f"シート '{entry_month}' が見つかりません。")
        return False

    # 正しいセル位置を計算して更新
    row = int(course_id) + 1
    column = entry_time.day + 1
    sheet_to_update.update_cell(row, column, result)
    print(f"出席記録: {course['class_name']} - {entry_label} - 結果: {result}")
    return True

# 全学生の出席を記録
def record_attendance(students_data, courses_data):
    """
    全学生の出席を記録する。
    """
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    student_index_data = students_data.get('student_info', {}).get('student_index', {})
    courses_list = courses_data.get('course_id', [])  # courses_list はリスト

    for student_id, attendance in attendance_data.items():
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。")
            continue

        student_index = student_info.get('student_index')
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', [])  # コースIDのリスト

        sheet_id = student_index_data.get(student_index, {}).get('sheet_id')
        if not sheet_id:
            print(f"学生インデックス {student_index} に対応するスプレッドシートIDが見つかりません。")
            continue

        # 学生のスプレッドシートを開く
        try:
            sheet = client.open_by_key(sheet_id)
        except Exception as e:
            print(f"スプレッドシート {sheet_id} を開けません: {e}")
            continue

        # 各コースの出席を確認
        for course_id in course_ids:
            try:
                course_id_int = int(course_id)  # インデックスとして利用するため整数に変換
                if course_id_int < 0 or course_id_int >= len(courses_list):
                    print(f"無効なコースID {course_id_int} が見つかりました。スキップします。")
                    continue

                course = courses_list[course_id_int]  # リストからコース情報を取得
                next_course = (
                    courses_list[course_id_int + 1]
                    if course_id_int + 1 < len(courses_list)
                    else None
                )

                # entry1/exit1の出席記録
                record_attendance_for_course(attendance, course, sheet, 'entry1', 'exit1', course_id)

                # entry2/exit2の出席記録
                if 'entry2' in attendance:
                    record_attendance_for_course(attendance, course, sheet, 'entry2', 'exit2', course_id)
            except (ValueError, IndexError) as e:
                print(f"エラー: {e}")
                continue

# Firebaseからデータを取得して出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
