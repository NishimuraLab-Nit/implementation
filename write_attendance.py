import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseの初期化
def initialize_firebase():
    print("[INFO] Firebaseの初期化を開始します...")
    if not firebase_admin._apps:
        try:
            cred = credentials.Certificate('/tmp/firebase_service_account.json')
            firebase_admin.initialize_app(cred, {
                'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
            })
            print("[SUCCESS] Firebaseが正常に初期化されました。")
        except Exception as e:
            print(f"[ERROR] Firebaseの初期化中にエラーが発生しました: {e}")
            raise
    else:
        print("[INFO] Firebaseは既に初期化されています。")

# Google Sheets APIの初期化
def initialize_google_sheets():
    print("[INFO] Google Sheets APIの初期化を開始します...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
        print("[SUCCESS] Google Sheets APIが正常に初期化されました。")
        return gspread.authorize(creds)
    except Exception as e:
        print(f"[ERROR] Google Sheets APIの初期化中にエラーが発生しました: {e}")
        raise

# Firebaseからデータ取得
def get_data_from_firebase(path):
    print(f"[INFO] Firebaseからデータを取得します: {path}")
    try:
        ref = db.reference(path)
        data = ref.get()
        if data:
            print(f"[SUCCESS] データ取得成功: {path}")
        else:
            print(f"[WARNING] {path} にデータが存在しません。")
        return data
    except Exception as e:
        print(f"[ERROR] Firebaseからデータを取得中にエラーが発生しました: {e}")
        return None

# 時刻を分単位に変換
def time_to_minutes(time_str):
    print(f"[INFO] 時刻 '{time_str}' を分単位に変換します...")
    try:
        time_obj = datetime.datetime.strptime(time_str, "%H:%M")
        minutes = time_obj.hour * 60 + time_obj.minute
        print(f"[SUCCESS] {time_str} → {minutes} 分")
        return minutes
    except Exception as e:
        print(f"[ERROR] 時刻変換中にエラーが発生しました: {e}")
        return None

# 出席判定ロジック
def determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes):
    print(f"[INFO] 出席判定を開始: entry={entry_minutes}, exit={exit_minutes}, start={start_minutes}, end={end_minutes}")
    if entry_minutes <= start_minutes + 5 and exit_minutes >= end_minutes - 5:
        print("[RESULT] 出席: ○")
        return "○"
    elif entry_minutes > start_minutes + 5 and exit_minutes >= end_minutes - 5:
        late_minutes = entry_minutes - start_minutes
        print(f"[RESULT] 遅刻: △遅{late_minutes}分")
        return f"△遅{late_minutes}分"
    elif entry_minutes <= start_minutes + 5 and exit_minutes < end_minutes - 5:
        early_minutes = end_minutes - exit_minutes
        print(f"[RESULT] 早退: △早{early_minutes}分")
        return f"△早{early_minutes}分"
    else:
        print("[RESULT] 欠席: ×")
        return "×"

# 出席記録を処理
def record_attendance(students_data, courses_data, client, sheet_names):
    print("[INFO] 出席記録処理を開始します...")
    attendance_data = students_data.get('attendance', {}).get('student_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    student_info_data = students_data.get('student_info', {}).get('student_id', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        print(f"\n[INFO] 学生ID: {student_id} の出席データを処理中...")
        student_index = student_info_data.get(student_id, {}).get('student_index')
        if not student_index:
            print(f"[WARNING] 学生ID {student_id} に対応する student_index が見つかりません。スキップします。")
            continue

        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', "").split(", ")
        sheet_id = students_data.get('student_info', {}).get('student_index', {}).get(student_index, {}).get('sheet_id')

        if not sheet_id:
            print(f"[WARNING] 学生ID {student_id} に対応する sheet_id が見つかりません。スキップします。")
            continue

        spreadsheet = client.open_by_key(sheet_id)
        print(f"[INFO] スプレッドシート '{sheet_id}' を取得しました。")
        for course_index, course_id in enumerate(course_ids, start=1):
            print(f"[INFO] 処理中: コースID {course_id} (Index: {course_index})...")
            if not course_id.isdigit() or int(course_id) >= len(courses_list):
                print(f"[ERROR] 無効なコースID {course_id} が見つかりました。スキップします。")
                continue

            course = courses_list[int(course_id)]
            schedule = course.get('schedule', {}).get('time', '').split('~')
            if len(schedule) != 2:
                print(f"[ERROR] コース {course_id} のスケジュール情報が不完全です。スキップします。")
                continue

            start_minutes = time_to_minutes(schedule[0])
            end_minutes = time_to_minutes(schedule[1])

            entry_time_str = attendance.get(f'entry{course_index}', {}).get('read_datetime')
            exit_time_str = attendance.get(f'exit{course_index}', {}).get('read_datetime')

            if not entry_time_str:
                print(f"[WARNING] 学生ID {student_id} の Entry 時間が見つかりません。スキップします。")
                continue

            entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
            entry_minutes = time_to_minutes(entry_time.strftime("%H:%M"))
            exit_minutes = time_to_minutes(exit_time_str.split(" ")[1]) if exit_time_str else None

            if exit_minutes is None:
                print(f"[INFO] Exit 時間が見つかりません。デフォルト値を使用します。")
                exit_minutes = end_minutes
                new_entry_time = entry_time + datetime.timedelta(minutes=10)
                new_exit_time = entry_time + datetime.timedelta(minutes=30)
                print(f"[INFO] 新しいEntry/ExitデータをFirebaseに保存します...")
                db.reference(f'Students/attendance/student_id/{student_id}/entry{course_index + 1}').set({
                    "read_datetime": new_entry_time.strftime("%Y-%m-%d %H:%M:%S")
                })
                db.reference(f'Students/attendance/student_id/{student_id}/exit{course_index + 1}').set({
                    "read_datetime": new_exit_time.strftime("%Y-%m-%d %H:%M:%S")
                })

            result = determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes)

            # 対応するシートを取得
            sheet_name = f"{entry_time.strftime('%Y-%m')}"
            worksheet = None
            for sheet in sheet_names:
                if sheet_name in sheet:
                    worksheet = spreadsheet.worksheet(sheet)
                    break

            if not worksheet:
                print(f"[WARNING] シート '{sheet_name}' が見つかりません。スキップします。")
                continue

            column = entry_time.day + 1
            row = course_index + 1
            print(f"[INFO] シート '{sheet_name}' のセル({row}, {column})に '{result}' を記録します...")
            worksheet.update_cell(row, column, result)
            print(f"[SUCCESS] '{result}' を記録しました！")

# メイン処理
def main():
    print("[INFO] メイン処理を開始します...")
    try:
        initialize_firebase()
        client = initialize_google_sheets()

        print("[INFO] Firebaseから学生データとコースデータを取得します...")
        students_data = get_data_from_firebase('Students')
        courses_data = get_data_from_firebase('Courses')

        if not students_data or not courses_data:
            print("[ERROR] 必要なデータが取得できませんでした。処理を中断します。")
            return

        print("[INFO] Googleスプレッドシートのシート名一覧を取得します...")
        sheet_names = [sheet.title for sheet in client.open_by_key('1aFhHFsK9Erqc54PQEmQUPXOCMpWzG5C2BsX3lda6KO4').worksheets()]
        print(f"[INFO] シート名一覧: {sheet_names}")

        print("[INFO] 出席記録を処理します...")
        record_attendance(students_data, courses_data, client, sheet_names)
        print("[SUCCESS] 出席記録処理が完了しました。")
    except Exception as e:
        print(f"[ERROR] メイン処理中にエラーが発生しました: {e}")

if __name__ == "__main__":
    main()
