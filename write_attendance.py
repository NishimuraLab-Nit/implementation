import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseアプリの初期化
def initialize_firebase():
    if not firebase_admin._apps:
        try:
            cred = credentials.Certificate('/tmp/firebase_service_account.json')
            firebase_admin.initialize_app(cred, {
                'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
            })
        except Exception as e:
            raise RuntimeError(f"Firebaseの初期化に失敗しました: {e}")

# Google Sheets APIの初期化
def initialize_gspread():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
        return gspread.authorize(creds)
    except Exception as e:
        raise RuntimeError(f"Google Sheets APIの認証に失敗しました: {e}")

# Firebaseデータ取得関数
def get_data_from_firebase(path):
    try:
        ref = db.reference(path)
        return ref.get()
    except Exception as e:
        print(f"Firebaseからデータを取得できません（パス: {path}）: {e}")
        return {}

# 出席判定ロジック
def check_attendance(entry_time_str, exit_time_str, schedule):
    try:
        # 入室時間と退室時間を解析
        entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
        entry_minutes = entry_time.hour * 60 + entry_time.minute

        exit_minutes = None
        if exit_time_str:
            exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
            exit_minutes = exit_time.hour * 60 + exit_time.minute

        # スケジュールの開始・終了時間を解析
        start_time_str, end_time_str = schedule.get('time', '').split('~')
        start_time = datetime.datetime.strptime(start_time_str, "%H:%M")
        end_time = datetime.datetime.strptime(end_time_str, "%H:%M")
        start_minutes = start_time.hour * 60 + start_time.minute
        end_minutes = end_time.hour * 60 + end_time.minute

        # 判定ロジック
        if entry_minutes <= start_minutes + 5:
            if exit_minutes is None or exit_minutes >= end_minutes - 5:
                return "〇", 0  # 正常出席
            elif exit_minutes < end_minutes - 5:
                early_minutes = end_minutes - exit_minutes
                return f"△早{early_minutes}分", early_minutes  # 早退
        elif entry_minutes > start_minutes + 5:
            if exit_minutes is None or exit_minutes >= end_minutes - 5:
                delay_minutes = entry_minutes - start_minutes
                return f"△遅{delay_minutes}分", delay_minutes  # 遅刻
        if entry_minutes > end_minutes:
            return "×", 0  # 欠席
        return None, 0
    except Exception as e:
        print(f"出席判定中にエラーが発生しました: {e}")
        return None, 0

# 出席を確認し、シートに記録する関数
def check_and_mark_attendance(attendance, course, sheet, entry_label, course_id):
    try:
        # 入退室データを取得
        entry_time_str = attendance.get(entry_label, {}).get('read_datetime')
        exit_label = entry_label.replace('entry', 'exit')
        exit_time_str = attendance.get(exit_label, {}).get('read_datetime')

        if not entry_time_str:
            print(f"{entry_label} に入室データが存在しません。スキップします。")
            return False  # 入室データがない場合スキップ

        # コースのスケジュール情報を取得
        schedule = course.get('schedule', {})
        if not schedule or 'time' not in schedule:
            print(f"コーススケジュール情報が見つかりません。スキップします。")
            return False

        # 判定を実行
        result, minutes = check_attendance(entry_time_str, exit_time_str, schedule)

        # スプレッドシートの書き込み準備
        entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
        entry_month = entry_time.strftime("%Y-%m")
        column = entry_time.day + 1  # 日付を列番号に変換

        try:
            sheet_to_update = sheet.worksheet(entry_month)
        except gspread.exceptions.WorksheetNotFound:
            print(f"シート '{entry_month}' が見つかりません。スキップします。")
            return False

        # 正しいセル位置を計算
        row = int(course_id) + 1

        # 結果を記録
        if result:
            sheet_to_update.update_cell(row, column, result)
            print(f"{course['class_name']} - {entry_label}: {result} を記録しました。")
        return True
    except Exception as e:
        print(f"出席の確認中にエラーが発生しました: {e}")
        return False

# 出席を記録する関数
def record_attendance(students_data, courses_data, client):
    try:
        attendance_data = students_data.get('attendance', {}).get('students_id', {})
        student_info_data = students_data.get('student_info', {}).get('student_id', {})
        courses_list = courses_data.get('course_id', [])

        for student_id, attendance in attendance_data.items():
            student_info = student_info_data.get(student_id)
            if not student_info:
                print(f"学生 {student_id} の情報が見つかりません。")
                continue

            student_index = student_info.get("student_index")
            sheet_id = students_data.get('student_info', {}).get('student_index', {}).get(student_index, {}).get("sheet_id")
            if not sheet_id:
                print(f"学生 {student_id} に対応するスプレッドシートIDが見つかりません。")
                continue

            try:
                sheet = client.open_by_key(sheet_id)
            except Exception as e:
                print(f"スプレッドシート {sheet_id} を開けません: {e}")
                continue

            course_ids = students_data.get('student_info', {}).get('student_index', {}).get(student_index, {}).get('course_id', [])
            for course_id in course_ids:
                if isinstance(course_id, str) and course_id.isdigit():
                    course = courses_list[int(course_id)]
                else:
                    print(f"コースID {course_id} に対応する授業が見つかりません。")
                    continue

                check_and_mark_attendance(attendance, course, sheet, 'entry1', course_id)
                if 'entry2' in attendance:
                    check_and_mark_attendance(attendance, course, sheet, 'entry2', course_id)
    except Exception as e:
        print(f"出席記録中にエラーが発生しました: {e}")

# メイン処理
def main():
    try:
        initialize_firebase()
        client = initialize_gspread()

        students_data = get_data_from_firebase('Students')
        courses_data = get_data_from_firebase('Courses')

        record_attendance(students_data, courses_data, client)
    except Exception as e:
        print(f"メイン処理中にエラーが発生しました: {e}")

if __name__ == "__main__":
    main()
