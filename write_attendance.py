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
            print(f"[エラー] Firebase初期化中にエラーが発生しました: {e}")
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
        print(f"[エラー] Google Sheets API初期化中にエラーが発生しました: {e}")
        raise

# Firebaseからデータを取得する関数
def get_data_from_firebase(path):
    try:
        print(f"Firebaseから'{path}'のデータを取得します...")
        ref = db.reference(path)
        data = ref.get()
        if data:
            print(f"[成功] '{path}'のデータを取得しました: {data}")
        else:
            print(f"[注意] '{path}'のデータが空です。")
        return data
    except Exception as e:
        print(f"[エラー] Firebaseからデータを取得中にエラーが発生しました: {e}")
        return None

# Googleスプレッドシートのシート名一覧を取得
def get_sheet_names(client, spreadsheet_id):
    try:
        print(f"スプレッドシート（ID: {spreadsheet_id}）のシート名を取得します...")
        spreadsheet = client.open_by_key(spreadsheet_id)
        sheet_names = [sheet.title for sheet in spreadsheet.worksheets()]
        print(f"[成功] 取得したシート名一覧: {sheet_names}")
        return sheet_names
    except Exception as e:
        print(f"[エラー] シート名の取得中にエラーが発生しました: {e}")
        return []

# 時刻を分単位に変換
def time_to_minutes(time_str):
    try:
        print(f"時刻文字列 '{time_str}' を分単位に変換します...")
        time_obj = datetime.datetime.strptime(time_str, "%H:%M")
        minutes = time_obj.hour * 60 + time_obj.minute
        print(f"[成功] 変換結果: {minutes} 分")
        return minutes
    except Exception as e:
        print(f"[エラー] 時刻変換中にエラーが発生しました: {e}")
        return None

# 出席を記録
def record_attendance(students_data, courses_data, client, spreadsheet_id, sheet_names):
    if not students_data or not courses_data:
        print("[エラー] 学生データまたはコースデータが存在しません。")
        return

    print("\n出席記録を開始します。")
    attendance_data = students_data.get('attendance', {}).get('student_id', {})
    enrollment_data = students_data.get('enrollment', {}).get('student_index', {})
    student_info_data = students_data.get('student_info', {}).get('student_id', {})
    courses_list = courses_data.get('course_id', [])

    spreadsheet = client.open_by_key(spreadsheet_id)

    for student_id, attendance in attendance_data.items():
        print(f"\n[処理中] 学生ID: {student_id}")
        student_info = student_info_data.get(student_id)
        if not student_info:
            print(f"[警告] 学生 {student_id} の情報が見つかりません。スキップします。")
            continue

        student_index = student_info.get('student_index')
        enrollment_info = enrollment_data.get(student_index, {})
        course_ids = enrollment_info.get('course_id', "").split(", ")

        print(f"[情報] 学生 {student_id} のコースID一覧: {course_ids}")

        for course_index, course_id in enumerate(course_ids, start=1):
            print(f"[処理中] コースID: {course_id}")
            try:
                course = courses_list[int(course_id)]
            except (ValueError, IndexError):
                print(f"[エラー] 無効なコースID {course_id} が見つかりました。スキップします。")
                continue

            entry_time_str = attendance.get(f'entry{course_index}', {}).get('read_datetime')
            if not entry_time_str:
                print(f"[警告] 学生 {student_id} のエントリー時間が見つかりません。スキップします。")
                continue

            # 日付から月を取得
            entry_time = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
            entry_month = entry_time.strftime("%m")

            # 月に一致するシートを探す
            matching_sheet = None
            for sheet_name in sheet_names:
                if entry_month in sheet_name:
                    matching_sheet = sheet_name
                    break

            if not matching_sheet:
                print(f"[警告] 月 '{entry_month}' に一致するシートが見つかりません。スキップします。")
                continue

            print(f"[成功] シート '{matching_sheet}' にデータを記録します。")

            # シートを開く
            worksheet = spreadsheet.worksheet(matching_sheet)

            # 列と行を計算
            column = entry_time.day + 1
            row = course_index + 1

            # データを記録
            try:
                worksheet.update_cell(row, column, entry_time.strftime("%H:%M"))
                print(f"[成功] シート '{matching_sheet}' のセル（{row}, {column}）にデータを記録しました。")
            except Exception as e:
                print(f"[エラー] データ記録中にエラーが発生しました: {e}")

# メイン処理
def main():
    try:
        print("[処理開始] FirebaseとGoogle Sheets APIの初期化を実行します...")
        initialize_firebase()
        client = initialize_google_sheets()

        print("[処理開始] Firebaseから学生データとコースデータを取得します...")
        students_data = get_data_from_firebase('Students')
        courses_data = get_data_from_firebase('Courses')

        print("[処理開始] Googleスプレッドシートのシート名を取得します...")
        spreadsheet_id = "1aFhHFsK9Erqc54PQEmQUPXOCMpWzG5C2BsX3lda6KO4"
        sheet_names = get_sheet_names(client, spreadsheet_id)

        print("[処理開始] 出席記録を処理します...")
        record_attendance(students_data, courses_data, client, spreadsheet_id, sheet_names)
    except Exception as e:
        print(f"[エラー] メイン処理中にエラーが発生しました: {e}")

if __name__ == "__main__":
    main()
