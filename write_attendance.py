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

# 出席を確認しマークする関数
def check_and_mark_attendance(attendance, course, sheet, entry_label, course_id):
    # 入室時間を取得
    entry_time_str = attendance.get(entry_label, {}).get('read_datetime')
    exit_time_str = attendance.get('exit', {}).get('read_datetime')

    # 入退室データがない場合は終了
    if not entry_time_str:
        return False

    # 入室時間を日付オブジェクトに変換
    entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
    entry_minutes = entry_time.hour * 60 + entry_time.minute

    # 退室時間を日付オブジェクトに変換（存在する場合のみ）
    exit_time = None
    if exit_time_str:
        exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
        exit_minutes = exit_time.hour * 60 + exit_time.minute
    else:
        exit_minutes = None

    # コースのスケジュール情報を取得
    schedule = course.get('schedule', {})
    start_time_str = schedule.get('time', '').split('~')[0]
    end_time_str = schedule.get('time', '').split('~')[1]

    # 開始・終了時刻を日付オブジェクトに変換
    start_time = datetime.datetime.strptime(start_time_str, "%H:%M")
    end_time = datetime.datetime.strptime(end_time_str, "%H:%M")
    start_minutes = start_time.hour * 60 + start_time.minute
    end_minutes = end_time.hour * 60 + end_time.minute

    # 対象シートの年月名を取得
    entry_month = entry_time.strftime("%Y-%m")
    column = entry_time.day + 1  # 日付を列番号に変換

    # シートを取得
    try:
        sheet_to_update = sheet.worksheet(entry_month)
    except gspread.exceptions.WorksheetNotFound:
        print(f"シート '{entry_month}' が見つかりません。スキップします。")
        return False

    # 正しいセル位置を計算
    row = int(course_id) + 1  # コースIDが1行目から開始すると仮定

    # 仕様に基づく判定ロジック
    if entry_minutes > end_minutes:  # entryが終了時間を過ぎていた場合
        sheet_to_update.update_cell(row, column, "×")
        print(f"{course['class_name']} - {entry_label}: 終了時間を過ぎています。マーク: ×")
        return True

    if exit_minutes is not None:
        if exit_minutes <= start_minutes + 5 and entry_minutes > start_minutes + 5:
            # exitが5分以内 & entryが開始時間の5分以内でない場合
            delay_minutes = entry_minutes - start_minutes
            sheet_to_update.update_cell(row, column, f"△遅{delay_minutes}分")
            print(f"{course['class_name']} - {entry_label}: 遅刻 {delay_minutes}分。マーク: △遅")
            return True

        if entry_minutes <= start_minutes + 5 and exit_minutes < end_minutes - 5:
            # entryが5分以内 & exitが終了時間の5分以内でない場合
            early_minutes = end_minutes - exit_minutes
            sheet_to_update.update_cell(row, column, f"△早{early_minutes}分")
            print(f"{course['class_name']} - {entry_label}: 早退 {early_minutes}分。マーク: △早")
            return True

    # entryしかない場合
    if not exit_time_str:
        # 終了時間をexitとして保存
        attendance['exit'] = {'read_datetime': entry_time.strftime("%Y-%m-%d ") + end_time.strftime("%H:%M:%S")}
        sheet_to_update.update_cell(row, column, "〇")
        print(f"{course['class_name']} - {entry_label}: 退室データなし。自動補完でマーク: 〇")
        return True

    # 上記以外の場合は〇をマーク
    sheet_to_update.update_cell(row, column, "〇")
    print(f"{course['class_name']} - {entry_label}: 正常出席。マーク: 〇")
    return True

def record_attendance(students_data, courses_data):
    # 各データを取得
    attendance_data = students_data.get('attendance', {})
    student_info_data = students_data.get('student_info', {})
    courses_list = courses_data.get('course_id', [])

    # 各学生の出席を確認
    for student_id, attendance in attendance_data.get("students_id", {}).items():
        # 学生情報を取得
        student_info = student_info_data.get("student_id", {}).get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。")
            continue

        student_index = student_info.get("student_index")
        sheet_id = student_info_data.get("student_index", {}).get(student_index, {}).get("sheet_id")
        if not sheet_id:
            print(f"学生 {student_id} に対応するスプレッドシートIDが見つかりません。")
            continue

        # スプレッドシートを開く
        try:
            sheet = client.open_by_key(sheet_id)
        except Exception as e:
            print(f"スプレッドシート {sheet_id} を開けません: {e}")
            continue

        # 受講しているコースを確認
        course_ids = student_info_data.get("student_index", {}).get(student_index, {}).get('course_id', [])
        for course_id in course_ids:  # course_ids はリスト型
            if isinstance(course_id, str) and course_id.isdigit():
                course = courses_list[int(course_id)]
            else:
                course = None
            if not course:
                print(f"コースID {course_id} に対応する授業が見つかりません。")
                continue

            # entry1とentry2の出席を確認
            if check_and_mark_attendance(attendance, course, sheet, 'entry1', course_id):
                continue
            if 'entry2' in attendance:
                check_and_mark_attendance(attendance, course, sheet, 'entry2', course_id)

# Firebaseからデータを取得し、出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
