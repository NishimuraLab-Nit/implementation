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

def check_and_mark_attendance(attendance, course, sheet, entry_label, exit_label, course_id):
    # 入室時間を取得
    entry_time_str = attendance.get(entry_label, {}).get('read_datetime')
    if not entry_time_str:
        return False  # 入室時間がない場合はスキップ

    # 退室時間を取得
    exit_time_str = attendance.get(exit_label, {}).get('read_datetime', None)

    # 入室時間と終了時間の解析
    entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
    entry_minutes = entry_time.hour * 60 + entry_time.minute

    # シート名（月単位）と曜日を取得
    entry_month = entry_time.strftime("%Y-%m")
    entry_day = entry_time.strftime("%A")

    # コースのスケジュールを取得
    schedule = course.get('schedule', {})
    if schedule.get('day') != entry_day:
        return False  # 曜日が一致しない場合はスキップ

    # 開始時間と終了時間を取得
    start_time_str, end_time_str = schedule.get('time', '').split('~')
    start_time = datetime.datetime.strptime(start_time_str, "%H:%M")
    start_minutes = start_time.hour * 60 + start_time.minute
    end_time = datetime.datetime.strptime(end_time_str, "%H:%M")
    end_minutes = end_time.hour * 60 + end_time.minute

    # 入室時間が終了時間より後であれば欠席
    if entry_minutes > end_minutes:
        record_to_sheet(sheet, "×", course_id, entry_month, entry_time.day)
        return False

    # 退室時間がない場合、終了時間を退室時間として保存
    if not exit_time_str:
        exit_time = end_time
        attendance[exit_label] = {"read_datetime": exit_time.strftime("%Y-%m-%d %H:%M:%S")}
    else:
        exit_time = datetime.datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")

    exit_minutes = exit_time.hour * 60 + exit_time.minute

    # 正常出席（〇）の判定
    if entry_minutes <= start_minutes + 5 and exit_minutes >= end_minutes - 5:
        record_to_sheet(sheet, "○", course_id, entry_month, entry_time.day)
        return True

    # 遅刻（△遅）の判定
    if entry_minutes > start_minutes + 5 and exit_minutes >= end_minutes - 5:
        late_minutes = entry_minutes - (start_minutes + 5)
        record_to_sheet(sheet, f"△遅{late_minutes}分", course_id, entry_month, entry_time.day)
        return True

    # 早退（△早）の判定
    if entry_minutes <= start_minutes + 5 and exit_minutes < end_minutes - 5:
        early_minutes = (end_minutes - 5) - exit_minutes
        record_to_sheet(sheet, f"△早{early_minutes}分", course_id, entry_month, entry_time.day)
        return True

    # 同教室で次のスケジュールの判定
    next_start_minutes = end_minutes
    next_end_minutes = next_start_minutes + (end_minutes - start_minutes)  # 仮の次スケジュール終了時間

    if exit_minutes >= next_end_minutes - 5:
        record_to_sheet(sheet, "○", course_id, entry_month, entry_time.day + 1)
    elif exit_minutes < next_end_minutes - 5:
        record_to_sheet(sheet, f"△早{next_end_minutes - 5 - exit_minutes}分", course_id, entry_month,
                        entry_time.day + 1)
    else:
        record_to_sheet(sheet, "×", course_id, entry_month, entry_time.day + 1)

    return False


def record_to_sheet(sheet, value, course_id, entry_month, day):
    """
    スプレッドシートに値を記録するユーティリティ関数
    """
    try:
        sheet_to_update = sheet.worksheet(entry_month)
    except gspread.exceptions.WorksheetNotFound:
        print(f"シート '{entry_month}' が見つかりません。スキップします。")
        return

    # 対象セルを計算して更新
    row = int(course_id) + 1
    column = day + 1
    sheet_to_update.update_cell(row, column, value)
    print(f"記録: {value} - コースID: {course_id} - シート: {entry_month}")

# Firebaseからデータを取得し、出席を記録
students_data = get_data_from_firebase('Students')
courses_data = get_data_from_firebase('Courses')
record_attendance(students_data, courses_data)
