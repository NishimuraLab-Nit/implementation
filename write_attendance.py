import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# Firebaseの初期化
def initialize_firebase():
    """Firebaseアプリの初期化を行います。"""
    if not firebase_admin._apps:
        cred = credentials.Certificate('/tmp/firebase_service_account.json')
        firebase_admin.initialize_app(cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })


# Google Sheets APIの初期化
def initialize_google_sheets():
    """Google Sheets APIの初期化を行います。"""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
    return gspread.authorize(creds)


# Firebaseからデータを取得
def get_data_from_firebase(path):
    """指定されたパスからFirebaseのデータを取得します。"""
    ref = db.reference(path)
    return ref.get()


# 時刻関連のユーティリティ関数
def time_to_minutes(time_str):
    """時刻文字列を分単位に変換します。"""
    time_obj = datetime.datetime.strptime(time_str, "%H:%M")
    return time_obj.hour * 60 + time_obj.minute


def minutes_to_time(minutes):
    """分を時刻文字列に変換します。"""
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02}:{mins:02}"


# Firebaseに時刻を保存
def save_time_to_firebase(path, time_obj):
    """指定されたパスに時刻データをFirebaseに保存します。"""
    ref = db.reference(path)
    ref.set({'read_datetime': time_obj.strftime("%Y-%m-%d %H:%M:%S")})


# 出席判定ロジック
def determine_attendance(entry_minutes, exit_minutes, start_minutes, end_minutes, student_id, course_index):
    """出席状況を判定します。"""
    transition_occurred = False

    if exit_minutes > end_minutes + 5:
        # 遅い退室処理
        final_exit_obj = create_time_object(end_minutes)
        save_time_to_firebase(f"Students/attendance/student_id/{student_id}/exit{course_index}", final_exit_obj)

        new_entry_time = end_minutes + 5
        new_entry_obj = create_time_object(new_entry_time)
        save_time_to_firebase(f"Students/attendance/student_id/{student_id}/entry{course_index + 1}", new_entry_obj)

        new_exit_obj = create_time_object(exit_minutes)
        save_time_to_firebase(f"Students/attendance/student_id/{student_id}/exit{course_index + 1}", new_exit_obj)

        transition_occurred = True
        return "○", transition_occurred, new_entry_obj, new_exit_obj

    # 判定ロジック
    if entry_minutes <= start_minutes + 5:
        if exit_minutes >= end_minutes - 5:
            return "○", transition_occurred, None, None
        return f"△早{end_minutes - 5 - exit_minutes}分", transition_occurred, None, None

    if exit_minutes >= end_minutes - 5:
        return f"△遅{entry_minutes - (start_minutes + 5)}分", transition_occurred, None, None

    return "×", transition_occurred, None, None


def create_time_object(minutes):
    """分を現在の日付の時刻オブジェクトに変換します。"""
    now = datetime.datetime.now()
    return now.replace(hour=minutes // 60, minute=minutes % 60, second=0, microsecond=0)


# 出席記録のメイン処理
def record_attendance(students_data, courses_data, sheet):
    """学生とコースデータを基に出席を記録します。"""
    if not students_data or not courses_data:
        print("学生データまたはコースデータが不足しています。")
        return

    attendance_data = students_data.get('attendance', {}).get('student_id', {})
    student_info_data = students_data.get('student_info', {}).get('student_id', {})
    courses_list = courses_data.get('course_id', [])

    sheet.append_row(["学生ID", "コースID", "判定結果", "移行"])

    for student_id, attendance in attendance_data.items():
        student_info = student_info_data.get(student_id)
        if not student_info:
            continue

        course_ids = student_info.get('course_id', "").split(", ")
        for course_index, course_id in enumerate(course_ids, start=1):
            course = courses_list[int(course_id)] if course_id.isdigit() else None
            if not course or 'schedule' not in course:
                continue

            start_minutes = time_to_minutes(course['schedule']['time'].split('~')[0])
            end_minutes = time_to_minutes(course['schedule']['time'].split('~')[1])

            entry_time_str = attendance.get(f'entry{course_index}', {}).get('read_datetime')
            exit_time_str = attendance.get(f'exit{course_index}', {}).get('read_datetime')

            if not entry_time_str:
                continue

            entry_minutes = time_to_minutes(entry_time_str.split()[-1])
            exit_minutes = time_to_minutes(exit_time_str.split()[-1]) if exit_time_str else end_minutes

            result, transition, new_entry_obj, new_exit_obj = determine_attendance(
                entry_minutes, exit_minutes, start_minutes, end_minutes, student_id, course_index
            )

            sheet.append_row([student_id, course_id, result, "Yes" if transition else "No"])


# メイン処理
def main():
    initialize_firebase()
    client = initialize_google_sheets()

    try:
        sheet = client.open("出席記録").sheet1
    except gspread.SpreadsheetNotFound:
        print("スプレッドシート '出席記録' が見つかりません。")
        return

    students_data = get_data_from_firebase('Students')
    courses_data = get_data_from_firebase('Courses')

    if not students_data or not courses_data:
        print("データが取得できませんでした。")
        return

    record_attendance(students_data, courses_data, sheet)


if __name__ == "__main__":
    main()
