import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseアプリの初期化
def initialize_firebase():
    if not firebase_admin._apps:
        print("Firebaseの初期化を実行します。")
        try:
            cred = credentials.Certificate('/tmp/firebase_service_account.json')
            firebase_admin.initialize_app(cred, {
                'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
            })
            print("Firebase初期化が完了しました。")
        except Exception as e:
            print(f"Firebase初期化中にエラーが発生しました: {e}")
            raise

# Google Sheets APIの初期化
def initialize_google_sheets():
    print("Google Sheets APIのスコープを設定します。")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
        print("Google Sheets APIの初期化が完了しました。")
        return gspread.authorize(creds)
    except Exception as e:
        print(f"Google Sheets API初期化中にエラーが発生しました: {e}")
        raise

# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    try:
        print(f"Firebaseから'{path}'のデータを取得します。")
        ref = db.reference(path)
        data = ref.get()
        print(f"'{path}'のデータ: {data}")
        return data
    except Exception as e:
        print(f"Firebaseからデータを取得中にエラーが発生しました: {e}")
        return None

# Googleスプレッドシートのシート名一覧を取得
def get_sheet_names(client, spreadsheet_id):
    try:
        print(f"スプレッドシート（ID: {spreadsheet_id}）のシート名を取得します。")
        spreadsheet = client.open_by_key(spreadsheet_id)
        sheet_names = [sheet.title for sheet in spreadsheet.worksheets()]
        print(f"取得したシート名一覧: {sheet_names}")
        return sheet_names
    except Exception as e:
        print(f"シート名の取得中にエラーが発生しました: {e}")
        return []

# 時刻を分単位に変換
def time_to_minutes(time_str):
    try:
        time_obj = datetime.datetime.strptime(time_str, "%H:%M")
        return time_obj.hour * 60 + time_obj.minute
    except Exception as e:
        print(f"時刻変換中にエラーが発生しました: {e}")
        return None

# 分単位を時刻文字列に変換
def minutes_to_time(minutes):
    try:
        hours = minutes // 60
        mins = minutes % 60
        return f"{hours:02}:{mins:02}"
    except Exception as e:
        print(f"分を時刻文字列に変換中にエラーが発生しました: {e}")
        return None

# Firebaseに時刻を保存
def save_time_to_firebase(path, time_obj):
    try:
        print(f"Firebaseにデータを保存します: {path} - {time_obj}")
        ref = db.reference(path)
        ref.set({'read_datetime': time_obj.strftime("%Y-%m-%d %H:%M:%S")})
        print(f"{path} に保存しました。")
    except Exception as e:
        print(f"Firebaseへの保存中にエラーが発生しました: {e}")

# 出席判定ロジック
def determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes):
    if entry_minutes <= start_minutes + 5 and exit_minutes >= end_minutes - 5:
        return "○"
    elif entry_minutes > start_minutes + 5 and exit_minutes >= end_minutes - 5:
        late_minutes = entry_minutes - (start_minutes + 5)
        return f"△遅{late_minutes}分"
    elif entry_minutes <= start_minutes + 5 and exit_minutes < end_minutes - 5:
        early_minutes = end_minutes - 5 - exit_minutes
        return f"△早{early_minutes}分"
    return "×"

# 出席を記録
def record_attendance(students_data, courses_data):
    if not students_data or not courses_data:
        print("学生データまたはコースデータが存在しません。")
        return

    print("\n出席記録を開始します。")
    attendance_data = students_data.get('attendance', {}).get('student_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    student_info_data = students_data.get('student_info', {}).get('student_id', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        print(f"\n学生ID: {student_id}")
        student_info = student_info_data.get(student_id)
        if not student_info:
            print(f"学生 {student_id} の情報が見つかりません。スキップします。")
            continue

        student_index = student_info.get('student_index')
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', "").split(", ")

        previous_entry = None
        previous_exit = None

        for course_index, course_id in enumerate(course_ids, start=1):
            try:
                course = courses_list[int(course_id)]
            except (ValueError, IndexError):
                print(f"無効なコースID {course_id} が見つかりました。スキップします。")
                continue

            schedule = course.get('schedule', {}).get('time', '').split('~')
            if len(schedule) != 2:
                print(f"コース {course_id} のスケジュール情報が不完全です。スキップします。")
                continue

            start_minutes = time_to_minutes(schedule[0])
            end_minutes = time_to_minutes(schedule[1])

            if previous_entry and previous_exit:
                entry_minutes = previous_entry
                exit_minutes = previous_exit
            else:
                entry_time_str = attendance.get(f'entry{course_index}', {}).get('read_datetime')
                exit_time_str = attendance.get(f'exit{course_index}', {}).get('read_datetime')

                if not entry_time_str or not exit_time_str:
                    print(f"学生 {student_id} のエントリーまたは退室データが見つかりません。スキップします。")
                    continue

                entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
                exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")

                entry_minutes = time_to_minutes(entry_time.strftime("%H:%M"))
                exit_minutes = time_to_minutes(exit_time.strftime("%H:%M"))

            result = determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes)
            print(f"学生 {student_id} のコース {course_id} の判定結果: {result}")

            previous_entry = entry_minutes
            previous_exit = exit_minutes

# メイン処理
def main():
    try:
        initialize_firebase()
        client = initialize_google_sheets()
        students_data = get_data_from_firebase('Students')
        courses_data = get_data_from_firebase('Courses')

        # シート名一覧を取得
        spreadsheet_id = "1aFhHFsK9Erqc54PQEmQUPXOCMpWzG5C2BsX3lda6KO4"
        sheet_names = get_sheet_names(client, spreadsheet_id)

        # 出席を記録
        record_attendance(students_data, courses_data)
    except Exception as e:
        print(f"メイン処理中にエラーが発生しました: {e}")

if __name__ == "__main__":
    main()
