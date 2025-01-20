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
            print(f"Firebase初期化中にエラーが発生しました: {e}")
            raise

# Google Sheets APIの初期化
def initialize_google_sheets():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
        return gspread.authorize(creds)
    except Exception as e:
        print(f"Google Sheets API初期化中にエラーが発生しました: {e}")
        raise

# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    try:
        ref = db.reference(path)
        return ref.get()
    except Exception as e:
        print(f"Firebaseからデータを取得中にエラーが発生しました: {e}")
        return None

# 時刻を分単位に変換
def time_to_minutes(time_str):
    try:
        time_obj = datetime.datetime.strptime(time_str, "%H:%M")
        return time_obj.hour * 60 + time_obj.minute
    except Exception as e:
        print(f"時刻変換中にエラーが発生しました: {e}")
        return None

# シートを取得または作成
def get_or_create_sheet(spreadsheet, sheet_name):
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"シート '{sheet_name}' が存在しません。新しく作成します。")
        return spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=20)

# 出席を記録
def record_attendance(students_data, courses_data, client):
    if not students_data or not courses_data:
        print("学生データまたはコースデータが存在しません。")
        return

    attendance_data = students_data.get('attendance', {}).get('student_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    student_info_data = students_data.get('student_info', {}).get('student_id', {})
    courses_list = courses_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        student_info = student_info_data.get(student_id)
        if not student_info:
            continue

        student_index = student_info.get('student_index')
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', "").split(", ")

        for course_index, course_id in enumerate(course_ids, start=1):
            try:
                course = courses_list[int(course_id)]
            except (ValueError, IndexError):
                continue

            schedule = course.get('schedule', {}).get('time', '').split('~')
            if len(schedule) != 2:
                continue

            start_minutes = time_to_minutes(schedule[0])
            end_minutes = time_to_minutes(schedule[1])

            entry_key = f'entry{course_index}'
            entry_time_str = attendance.get(entry_key, {}).get('read_datetime')

            if not entry_time_str:
                continue

            entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
            sheet_name = entry_time.strftime("%Y-%m")

            try:
                spreadsheet = client.open_by_key(course.get('course_sheet_id'))
                sheet = get_or_create_sheet(spreadsheet, sheet_name)

                cell = sheet.find(student_index)
                if cell:
                    sheet.update_cell(cell.row, cell.col + 1, "○")
                    print(f"学生ID: {student_id} の出席情報を '{sheet_name}' シートに更新しました。")
                else:
                    print(f"学生インデックス {student_index} がシート内に見つかりませんでした。")
            except Exception as e:
                print(f"シート更新中にエラーが発生しました: {e} (シート名: {sheet_name})")

# メイン処理
def main():
    try:
        initialize_firebase()
        client = initialize_google_sheets()
        students_data = get_data_from_firebase('Students')
        courses_data = get_data_from_firebase('Courses')
        record_attendance(students_data, courses_data, client)
    except Exception as e:
        print(f"メイン処理中にエラーが発生しました: {e}")

if __name__ == "__main__":
    main()
