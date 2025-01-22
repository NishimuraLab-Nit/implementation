import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# Firebaseの初期化
def initialize_firebase():
    if not firebase_admin._apps:
        try:
            cred = credentials.Certificate('/tmp/firebase_service_account.json')
            firebase_admin.initialize_app(cred, {
                'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
            })
            print("[SUCCESS] Firebase initialized.")
        except Exception as e:
            raise RuntimeError(f"Error initializing Firebase: {e}")
    else:
        print("[INFO] Firebase is already initialized.")


# Google Sheets APIの初期化
def initialize_google_sheets():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
        return gspread.authorize(creds)
    except Exception as e:
        raise RuntimeError(f"Error initializing Google Sheets API: {e}")


# Firebaseからデータ取得
def get_data_from_firebase(path):
    try:
        ref = db.reference(path)
        data = ref.get()
        return data
    except Exception as e:
        raise RuntimeError(f"Error fetching data from Firebase ({path}): {e}")


# 時刻を分単位に変換
def time_to_minutes(time_str):
    try:
        time_obj = datetime.datetime.strptime(time_str, "%H:%M")
        return time_obj.hour * 60 + time_obj.minute
    except Exception as e:
        raise ValueError(f"Invalid time format ({time_str}): {e}")


# 出席判定ロジック
def determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes):
    if entry_minutes <= start_minutes + 5 and exit_minutes >= end_minutes - 5:
        return "○"
    elif entry_minutes > start_minutes + 5 and exit_minutes >= end_minutes - 5:
        return f"△遅{entry_minutes - start_minutes}分"
    elif entry_minutes <= start_minutes + 5 and exit_minutes < end_minutes - 5:
        return f"△早{end_minutes - exit_minutes}分"
    else:
        return "×"


# 出席記録を処理
def record_attendance(student_data, course_data, client, sheet_names):
    attendance_data = student_data.get('attendance', {}).get('student_id', {})
    enrollment_data = student_data.get('enrollment', {}).get('student_index', {})
    student_info_data = student_data.get('student_info', {}).get('student_id', {})
    courses_list = course_data.get('course_id', [])

    for student_id, attendance in attendance_data.items():
        student_index = student_info_data.get(student_id, {}).get('student_index')
        if not student_index:
            continue

        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', "").split(", ")
        sheet_id = student_data.get('student_info', {}).get('student_index', {}).get(student_index, {}).get('sheet_id')

        if not sheet_id:
            continue

        spreadsheet = client.open_by_key(sheet_id)

        for course_index, course_id in enumerate(course_ids, start=1):
            if not course_id.isdigit() or int(course_id) >= len(courses_list):
                continue

            course = courses_list[int(course_id)]
            schedule = course.get('schedule', {}).get('time', '').split('~')
            if len(schedule) != 2:
                continue

            start_minutes = time_to_minutes(schedule[0])
            end_minutes = time_to_minutes(schedule[1])

            entry_time_str = attendance.get(f'entry{course_index}', {}).get('read_datetime')
            exit_time_str = attendance.get(f'exit{course_index}', {}).get('read_datetime')

            if not entry_time_str:
                continue

            entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
            entry_minutes = time_to_minutes(entry_time.strftime("%H:%M"))
            exit_minutes = time_to_minutes(exit_time_str.split(" ")[1]) if exit_time_str else end_minutes

            result = determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes)

            sheet_name = f"{entry_time.strftime('%Y-%m')}"
            worksheet = next((sheet for sheet in sheet_names if sheet_name in sheet), None)

            if worksheet:
                column = entry_time.day + 1
                row = course_index + 1
                worksheet.update_cell(row, column, result)


# メイン処理
def main():
    try:
        initialize_firebase()
        client = initialize_google_sheets()

        students_data = get_data_from_firebase('Students')
        courses_data = get_data_from_firebase('Courses')

        if not students_data or not courses_data:
            raise RuntimeError("Failed to fetch required data.")

        sheet_names = [sheet.title for sheet in client.open_by_key('1aFhHFsK9Erqc54PQEmQUPXOCMpWzG5C2BsX3lda6KO4').worksheets()]

        record_attendance(students_data, courses_data, client, sheet_names)
        print("[SUCCESS] Attendance processing completed.")
    except Exception as e:
        print(f"[ERROR] Main process error: {e}")


if __name__ == "__main__":
    main()
