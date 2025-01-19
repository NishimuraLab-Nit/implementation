import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# FirebaseとGoogle Sheetsの初期化
def initialize_services():
    """FirebaseとGoogle Sheetsのサービスを初期化する"""
    # Firebase初期化（未初期化の場合のみ）
    if not firebase_admin._apps:
        cred = credentials.Certificate('/tmp/firebase_service_account.json')
        firebase_admin.initialize_app(cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })

    # Google Sheets APIの認証
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
    return gspread.authorize(creds)

# Firebaseからデータ取得
def get_data_from_firebase(path):
    """Firebaseから指定されたパスのデータを取得する"""
    ref = db.reference(path)
    data = ref.get()
    if data is None:
        print(f"Firebaseからデータが見つかりません: {path}")
    return data

# 出席判定ロジック
def determine_attendance_status(entry_time, exit_time, start_time, end_time):
    """
    出席状況を判定する

    Parameters:
        entry_time: 入室時間
        exit_time: 退室時間
        start_time: 授業開始時間
        end_time: 授業終了時間
    """
    # 日付を入室日付で統一
    today = entry_time.date()
    start_time = datetime.datetime.combine(today, start_time.time())
    end_time = datetime.datetime.combine(today, end_time.time())

    # 判定基準を計算
    start_plus_5 = start_time + datetime.timedelta(minutes=5)
    end_minus_5 = end_time - datetime.timedelta(minutes=5)
    end_plus_15 = end_time + datetime.timedelta(minutes=15)

    # 出席判定
    if entry_time <= start_plus_5:
        if not exit_time or exit_time >= end_minus_5:
            return "○"  # 正常出席
        elif exit_time < end_minus_5:
            early_minutes = (end_minus_5 - exit_time).seconds // 60
            return f"△早{early_minutes}分"  # 早退
    elif entry_time > start_plus_5:
        if not exit_time or exit_time >= end_minus_5:
            late_minutes = (entry_time - start_plus_5).seconds // 60
            return f"△遅{late_minutes}分"  # 遅刻
    if entry_time > end_time:
        return "×"  # 欠席
    if entry_time <= start_plus_5 and exit_time >= end_plus_15:
        return "○ 同教室"  # 同教室判定
    return "×"  # デフォルトは欠席

# 出席データをシートに記録
def check_and_mark_attendance(attendance, course, sheet, entry_label, course_id):
    """
    出席データを処理し、Google Sheetsに記録する

    Parameters:
        attendance: 出席データ
        course: コースデータ
        sheet: Google Sheetsオブジェクト
        entry_label: 入室ラベル（例: 'entry1'）
        course_id: コースID
    """
    entry_time_str = attendance.get(entry_label, {}).get('read_datetime')
    exit_time_str = attendance.get(entry_label, {}).get('exit_datetime')

    # 入室時間が存在しない場合はスキップ
    if not entry_time_str:
        return False

    # 入室・退室時間をパース
    entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
    exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S") if exit_time_str else None

    # スケジュール時間を取得
    schedule = course.get('schedule', {}).get('time')
    if not schedule:
        return False
    start_time_str, end_time_str = schedule.split('~')
    start_time = datetime.datetime.strptime(start_time_str, "%H:%M")
    end_time = datetime.datetime.strptime(end_time_str, "%H:%M")

    # 出席状況を判定
    status = determine_attendance_status(entry_time, exit_time, start_time, end_time)

    # 対象シートを取得（入室年月でシートを識別）
    entry_month = entry_time.strftime("%Y-%m")
    try:
        sheet_to_update = sheet.worksheet(entry_month)
    except gspread.exceptions.WorksheetNotFound:
        print(f"シート '{entry_month}' が見つかりません。スキップします。")
        return False

    # シートに出席状況を記録
    row = int(course_id) + 1
    column = entry_time.day + 1
    sheet_to_update.update_cell(row, column, status)
    print(f"出席記録: row={row}, column={column}, status={status}")
    return True

# 出席データの記録処理
def record_attendance(students_data, courses_data, client):
    """
    学生データとコースデータを処理し、出席状況を記録する

    Parameters:
        students_data: 学生データ
        courses_data: コースデータ
        client: Google Sheetsクライアント
    """
    attendance_data = students_data.get('attendance', {}).get('students_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    student_index_data = students_data.get('student_info', {}).get('student_index', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        student_info = students_data.get('student_info', {}).get('student_id', {}).get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。スキップします。")
            continue

        student_index = student_info.get('student_index')
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', [])

        sheet_id = student_index_data.get(student_index, {}).get('sheet_id')
        if not sheet_id:
            print(f"学生インデックス {student_index} に対応するスプレッドシートIDが見つかりません。スキップします。")
            continue

        # Google Sheetsを開く
        try:
            sheet = client.open_by_key(sheet_id)
        except Exception as e:
            print(f"スプレッドシート {sheet_id} を開けません: {e}")
            continue

        # 各コースについて出席データを記録
        for course_id in course_ids:
            try:
                course_id_int = int(course_id)
                course = courses_list[course_id_int]
            except (ValueError, IndexError):
                print(f"無効なコースID {course_id} が見つかりました。スキップします。")
                continue

            if check_and_mark_attendance(attendance, course, sheet, 'entry1', course_id):
                continue

            if 'entry2' in attendance:
                check_and_mark_attendance(attendance, course, sheet, 'entry2', course_id)

# メイン処理
def main():
    print("サービスを初期化中...")
    client = initialize_services()
    print("Firebaseからデータを取得中...")
    students_data = get_data_from_firebase('Students')
    courses_data = get_data_from_firebase('Courses')
    print("出席記録処理を開始します...")
    record_attendance(students_data, courses_data, client)
    print("出席記録処理が完了しました。")

if __name__ == "__main__":
    main()
