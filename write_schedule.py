import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebaseの初期化
def initialize_firebase():
    """Firebaseアプリの初期化を行います。"""
    if not firebase_admin._apps:
        print("Firebaseの初期化を実行します。")
        cred = credentials.Certificate('/tmp/firebase_service_account.json')
        firebase_admin.initialize_app(cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })
        print("Firebase初期化が完了しました。")

# Google Sheets APIの初期化
def initialize_google_sheets():
    """Google Sheets APIの初期化を行います。"""
    print("Google Sheets APIのスコープを設定します。")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
    print("Google Sheets APIの初期化が完了しました。")
    return gspread.authorize(creds)

# Firebaseからデータを取得
def get_data_from_firebase(path):
    """指定したパスからFirebaseのデータを取得します。"""
    print(f"Firebaseから'{path}'のデータを取得します。")
    ref = db.reference(path)
    data = ref.get()
    print(f"'{path}'のデータ: {data}")
    return data

# 時刻を分単位に変換
def time_to_minutes(time_str):
    """時刻文字列を分単位に変換します。"""
    print(f"'{time_str}'を分に変換します。")
    time_obj = datetime.datetime.strptime(time_str, "%H:%M")
    return time_obj.hour * 60 + time_obj.minute

# 分単位を時刻文字列に変換
def minutes_to_time(minutes):
    """分単位を時刻文字列に変換します。"""
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02}:{mins:02}"

# Firebaseに時刻を保存
def save_time_to_firebase(path, time_obj):
    """指定したパスに時刻データをFirebaseに保存します。"""
    print(f"Firebaseにデータを保存します: {path} - {time_obj}")
    ref = db.reference(path)
    ref.set({'read_datetime': time_obj.strftime("%Y-%m-%d %H:%M:%S")})
    print(f"{path} に保存しました。")

# Google Sheetsのシート名を取得
def get_sheet_by_entry_date(entry_datetime, client):
    """entryの日時を基にシート名を取得し、該当シートを返します。"""
    sheet_name = entry_datetime.strftime("%Y-%m")  # シート名を「%Y-%m」の形式で取得
    print(f"シート名を取得: {sheet_name}")
# 出席記録
def record_attendance(students_data, courses_data, client):
    """
    学生データとコースデータを基に出席を記録します。
    """
    if not students_data or not courses_data:
        print("学生データまたはコースデータが存在しません。")
        return

    attendance_data = students_data.get('attendance', {}).get('student_id', {})

    for student_id, attendance in attendance_data.items():
        print(f"\n学生ID: {student_id}")
        for entry_key in attendance.keys():
            if entry_key.startswith("entry"):
                entry_datetime_str = attendance[entry_key].get('read_datetime')
                if not entry_datetime_str:
                    print(f"学生 {student_id} の {entry_key} データが見つかりません。スキップします。")
                    continue

                # Entryの日時を解析
                entry_datetime = datetime.datetime.strptime(entry_datetime_str, "%Y-%m-%d %H:%M:%S")

                # 対応するシートを取得
                sheet = get_sheet_by_entry_date(entry_datetime, client)

                # 必要なデータをシートに追加
                sheet.append_row([student_id, entry_key, entry_datetime.strftime("%Y-%m-%d %H:%M:%S")])
                print(f"学生 {student_id} のエントリーデータをシート '{sheet.title}' に追加しました。")

# メイン処理
def main():
    initialize_firebase()
    client = initialize_google_sheets()

    students_data = get_data_from_firebase('Students')
    courses_data = get_data_from_firebase('Courses')

    if students_data is None or courses_data is None:
        print("Firebaseから必要なデータが取得できませんでした。")
        return

    record_attendance(students_data, courses_data, client)

if __name__ == "__main__":
    main()
