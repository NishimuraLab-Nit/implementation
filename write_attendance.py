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

# 出席判定と更新処理を行う関数
def check_and_mark_attendance(attendance, course, sheet, entry_label, exit_label, course_id, next_course=None):
    # 入室時間・退室時間を取得
    entry_time_str = attendance.get(entry_label, {}).get('read_datetime')
    exit_time_str = attendance.get(exit_label, {}).get('read_datetime', None)

    if not entry_time_str:
        return False

    # 入室時間を日付オブジェクトに変換
    entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
    entry_minutes = entry_time.hour * 60 + entry_time.minute

    # コースの開始・終了時間を取得
    start_time_str, end_time_str = course.get('schedule', {}).get('time', '').split('~')
    start_time = datetime.datetime.strptime(start_time_str, "%H:%M")
    end_time = datetime.datetime.strptime(end_time_str, "%H:%M")
    start_minutes = start_time.hour * 60 + start_time.minute
    end_minutes = end_time.hour * 60 + end_time.minute

    # 退室時間がある場合は変換、ない場合は終了時間を使用
    if exit_time_str:
        exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
        exit_minutes = exit_time.hour * 60 + exit_time.minute
    else:
        exit_time = end_time
        exit_minutes = end_minutes

    # 追加仕様の判定
    if exit_minutes > end_minutes + 15:
        print(f"退室時間が終了時間+15分以降です。処理を開始します。")
        # 退室時間1を別変数に保存
        original_exit_time = exit_time

        # 終了時間1を退室時間1に代入
        exit_time = end_time
        exit_minutes = end_minutes

        # 終了時間1+10分を入室時間2に代入
        new_entry_time = end_time + datetime.timedelta(minutes=10)

        # Firebaseに保存
        update_data_to_firebase(attendance, course_id, exit_time, new_entry_time)

        # 次のコースへ移行
        if next_course:
            print(f"正常出席として処理を完了し、コースID {course_id} から次のコースへ移行します。")
        return True

    # 判定ロジック
    result = ""
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
    elif entry_minutes >= end_minutes:  # 入室が「終了時間」より以降
        result = "×"  # 欠席

    # 対象のシートを取得し更新
    try:
        entry_month = entry_time.strftime("%Y-%m")  # シート名に使用する年月
        sheet_to_update = sheet.worksheet(entry_month)
    except gspread.exceptions.WorksheetNotFound:
        print(f"シート '{entry_month}' が見つかりません。スキップします。")
        return False

    # 正しいセル位置を計算して更新
    row = int(course_id) + 1
    column = entry_time.day + 1
    sheet_to_update.update_cell(row, column, result)
    print(f"出席記録: {course['class_name']} - {entry_label} - シート: {entry_month} - 結果: {result}")
    return True

# Firebaseにデータを保存する関数
def update_data_to_firebase(attendance, course_id, exit_time, new_entry_time):
    try:
        ref = db.reference(f"Students/attendance/students_id/{attendance}")
        ref.update({
            'exit1': {'read_datetime': exit_time.strftime("%Y-%m-%d %H:%M:%S")},
            'entry2': {'read_datetime': new_entry_time.strftime("%Y-%m-%d %H:%M:%S")}
        })
        print(f"Firebaseにデータを更新しました: コースID {course_id}")
    except Exception as e:
        print(f"Firebaseへの更新中にエラーが発生しました: {e}")

# 出席を記録する関数
def record_attendance(students_data, courses_data):
    # 各データを取得
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    courses_list = courses_data.get('course_id', [])

    # 各学生の出席を確認
    for student_id, attendance in attendance_data.items():
        # 学生情報を取得
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。")
            continue

        # スプレッドシートを開く
        student_index = student_info.get('student_index')
        sheet_id = students_data.get('student_info', {}).get('student_index', {}).get(student_index, {}).get('sheet_id')
        try:
            sheet = client.open_by_key(sheet_id)
        except Exception as e:
            print(f"スプレッドシート {sheet_id} を開けません: {e}")
            continue

        # 各コースの出席を確認
        for course_id in attendance.get('course_id', []):
            try:
                course = courses_list[int(course_id)]
                next_course = courses_list[int(course_id) + 1] if int(course_id) + 1 < len(courses_list) else None
            except (ValueError, IndexError):
                print(f"無効なコースID {course_id} が見つかりました。スキップします。")
                continue

            # entry1とexit1の出席を確認
            check_and_mark_attendance(attendance, course, sheet, 'entry1', 'exit1', course_id, next_course)

# Firebaseからデータを取得し、出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
