import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# Firebaseアプリの初期化（未初期化の場合のみ実行）
def initialize_firebase():
    if not firebase_admin._apps:
        print("Initializing Firebase app...")
        cred = credentials.Certificate('/tmp/firebase_service_account.json')
        firebase_admin.initialize_app(cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })


# Google Sheets APIのクライアントを初期化
def initialize_google_sheets():
    print("Authenticating Google Sheets API...")
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
    return gspread.authorize(creds)


# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    try:
        print(f"Fetching data from Firebase: Path={path}")
        ref = db.reference(path)
        data = ref.get()
        print(f"Data retrieved from {path}: {data}")
        return data
    except Exception as e:
        print(f"Error fetching data from Firebase: {e}")
        return None


# 時刻フォーマットの変換
def parse_time(time_str):
    try:
        print(f"Parsing time string: {time_str}")
        start, end = time_str.split("~")
        start_time = datetime.datetime.strptime(start, "%H%M")
        end_time = datetime.datetime.strptime(end, "%H%M")
        print(f"Parsed times - Start: {start_time}, End: {end_time}")
        return start_time, end_time
    except ValueError:
        print(f"Invalid time format: {time_str}")
        return None, None


# 日時フォーマットの変換
def parse_datetime(dt_str):
    try:
        print(f"Parsing datetime string: {dt_str}")
        dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        print(f"Parsed datetime: {dt}")
        return dt
    except ValueError:
        print(f"Invalid datetime format: {dt_str}")
        return None


# 出席データの判定処理
def evaluate_attendance(entry_time, exit_time, start_time, end_time):
    if entry_time is None or exit_time is None:
        return "✕"
    if entry_time > exit_time:
        return "✕"
    elif entry_time <= start_time + datetime.timedelta(minutes=5) and exit_time <= end_time + datetime.timedelta(minutes=5):
        return "〇"
    elif entry_time > start_time + datetime.timedelta(minutes=5) and exit_time <= end_time + datetime.timedelta(minutes=5):
        late_minutes = (entry_time - start_time).seconds // 60
        return f"△遅{late_minutes}分"
    elif entry_time <= start_time + datetime.timedelta(minutes=5) and exit_time < end_time - datetime.timedelta(minutes=5):
        early_leave_minutes = (end_time - exit_time).seconds // 60
        return f"△早{early_leave_minutes}分"
    else:
        return "✕"


# Googleシートの更新
def update_google_sheet(sheet, course_row, day_column, status):
    try:
        print(f"Updating Google Sheet: Row={course_row}, Column={day_column}, Status={status}")
        sheet.update_cell(course_row, day_column, status)
        print("Google Sheet updated successfully.")
    except Exception as e:
        print(f"Error updating Google Sheet: {e}")


# メイン処理
def main():
    print("Starting main process...")

    # 初期化
    initialize_firebase()
    client = initialize_google_sheets()

    # データ取得
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    student_info_data = get_data_from_firebase("Students/student_info/student_index")
    enrollment_data = get_data_from_firebase("Students/enrollment/student_index")
    courses_data = get_data_from_firebase("Courses").get("course_id", [])

    if not attendance_data or not student_info_data or not enrollment_data or not courses_data:
        print("Failed to fetch required data. Exiting...")
        return

    # 学生ごとの出席データを処理
    for student_id, attendance in attendance_data.items():
        print(f"Processing attendance data for Student ID: {student_id}...")
        student_index = get_data_from_firebase(f"Students/student_info/student_id/{student_id}/student_index")
    
        if not student_index:
            print(f"No student index found for Student ID: {student_id}")
            continue
    
        course_ids = enrollment_data.get(student_index, {}).get("course_id", "").split(", ")
        print(f"Student ID {student_id} is enrolled in courses: {course_ids}")
    
        for course_id in course_ids:
            if not course_id:
                print(f"No valid course ID found for Student ID: {student_id}")
                continue
    
            # コースデータの取得
            course_data = next(
                (course for course in courses_data if isinstance(course, dict) and str(course.get("id")) == course_id),
                None
            )
            if not course_data:
                print(f"No course data found for Course ID: {course_id}")
                continue
    
            # スケジュールの取得
            schedule = course_data.get("schedule", {}).get("time", "")
            if not schedule:
                print(f"No schedule found for Course ID: {course_id}")
                continue
    
            start_time, end_time = parse_time(schedule)
    
            # 出席データの判定と更新
            for entry_key, entry_data in attendance.items():
                if not entry_key.startswith("entry"):
                    continue
    
                entry_time = parse_datetime(entry_data["read_datetime"])
                exit_key = entry_key.replace("entry", "exit")
                exit_time = parse_datetime(attendance.get(exit_key, {}).get("read_datetime"))
    
                status = evaluate_attendance(entry_time, exit_time, start_time, end_time)
                print(f"Entry Time: {entry_time}, Exit Time: {exit_time}, Status: {status}")
    
                # Googleシートの更新
                sheet_id = student_info_data.get(student_index, {}).get("sheet_id", "")
                if sheet_id:
                    sheet = client.open_by_key(sheet_id).worksheet(datetime.datetime.now().strftime("%Y-%m"))
                    day_column = entry_time.day if entry_time else 0
                    course_row = int(course_id)
                    update_google_sheet(sheet, course_row, day_column, status)


if __name__ == "__main__":
    main()
