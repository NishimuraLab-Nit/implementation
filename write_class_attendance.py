import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------
# 定数定義
# ---------------------
# 各時限の時間帯 (開始, 終了) を datetime.time で定義
# 実運用では、分単位のゆらぎや厳密な比較など考慮が必要です
PERIOD_TIMES = {
    1: (datetime.time(8, 50),  datetime.time(10, 20)),
    2: (datetime.time(10, 30), datetime.time(12, 0)),
    3: (datetime.time(13, 10), datetime.time(14, 40)),
    4: (datetime.time(14, 50), datetime.time(16, 20))
}

# ---------------------
# Firebase & GSpread初期化
# ---------------------
def init_firebase_and_gspread():
    # Firebase初期化
    if not firebase_admin._apps:
        cred = credentials.Certificate("/tmp/firebase_service_account.json")
        firebase_admin.initialize_app(cred, {
            "databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/"
        })
        print("Firebase initialized.")

    # GSpread初期化
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/gcp_service_account.json", scope)
    gclient = gspread.authorize(creds)
    print("Google Sheets API authorized.")

    return gclient

# ---------------------
# Firebaseからデータ取得
# ---------------------
def get_data_from_firebase(path):
    """与えられたFirebase Realtime Databaseのパスからデータを取得"""
    print(f"Fetching data from Firebase path: {path}")
    ref = db.reference(path)
    data = ref.get()
    if data is None:
        print(f"No data found at path: {path}")
    return data

# ---------------------
# 現在日時 → 当日のシート名・列の算出
# ---------------------
def get_current_date_details():
    """
    例:
        now = 2025-01-26 09:35
        current_sheet_name = "2025-01"
        current_day_of_month = 26
    """
    now = datetime.datetime.now()
    current_sheet_name = now.strftime('%Y-%m')
    current_day_of_month = now.day
    return now, current_sheet_name, current_day_of_month

def get_current_period(now):
    """
    現在時刻がどの時限に当たるかを取得する。
    PERIOD_TIMES の範囲に合致しない場合は None を返す。
    """
    now_time = now.time()  # datetime.time オブジェクト
    for period, (start_t, end_t) in PERIOD_TIMES.items():
        if start_t <= now_time <= end_t:
            return period
    return None

def map_row_column(day_of_month, student_index_position, period):
    """
    row = 学生idxループの通し番号 + 2
    column = (日付×4) + period - 2
    """
    row = student_index_position + 2
    column = (day_of_month * 4) + period - 2
    return row, column

# ---------------------
# メイン処理
# ---------------------
def main():
    gclient = init_firebase_and_gspread()
    now, current_sheet_name, current_day_of_month = get_current_date_details()

    # ---- 1. どのクラスを処理するか ----
    # 例: class_index="E5" とする
    class_index = "E5"

    # Class配下のデータを取得
    class_data_path = f"Class/class_index/{class_index}"
    class_data = get_data_from_firebase(class_data_path)
    if not class_data:
        print(f"No class data found for class_index={class_index}")
        return

    # 例: class_data["student_index"] -> "E523, E534"
    #     class_data["class_sheet_id"] -> "xxxx..."
    student_indices_str = class_data.get("student_index")
    class_sheet_id = class_data.get("class_sheet_id")
    if not student_indices_str or not class_sheet_id:
        print("student_index または class_sheet_id が見つかりません。")
        return

    # student_index をリスト化
    student_indices = [s.strip() for s in student_indices_str.split(',')]
    print("Target student_indices:", student_indices)

    # ---- 2. 対象スプレッドシートを開く ----
    try:
        sh = gclient.open_by_key(class_sheet_id)
        print(f"Opened Google Sheet: {sh.title}")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Spreadsheet with ID {class_sheet_id} not found.")
        return

    # シート名を選択 (例: "2025-01")
    try:
        sheet = sh.worksheet(current_sheet_name)
        print(f"Using worksheet: {sheet.title}")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet named '{current_sheet_name}' not found in spreadsheet {class_sheet_id}.")
        return

    # ---- 3. 現在時刻がどの時限かを判定 ----
    #      ※ 「実行時刻に対応する時限だけ」を書き込む想定
    current_period = get_current_period(now)
    if current_period is None:
        print("現在時刻はどの時限にも該当しないため、書き込みを行いません。")
        return

    print(f"Current time: {now}, This is period {current_period}.")

    # ---- 4. 各 student_index ごとの処理 ----
    for idx, student_idx in enumerate(student_indices, start=1):
        print(f"\n=== Processing StudentIndex={student_idx} (index={idx}) ===")

        # 4-1. student_id を取得
        student_id_path = f"Students/student_info/student_index/{student_idx}/student_id"
        student_id = get_data_from_firebase(student_id_path)
        if not student_id:
            print(f"No student_id found for student_index {student_idx}. Skipping.")
            continue

        print(f"Found student_id={student_id} for student_index={student_idx}.")

        # 4-2. 学生の attendance を取得
        #      Students/attendance/student_id/{student_id}
        attendance_path = f"Students/attendance/student_id/{student_id}"
        attendance_data = get_data_from_firebase(attendance_path)
        if not attendance_data:
            print(f"No attendance data found for student_id={student_id}. Skipping.")
            continue

        # ---- 5. 現在時限に対応する entry/exit のキーを確認 ----
        #      例: period=2 なら entry2, exit2
        entry_key = f"entry{current_period}"
        exit_key = f"exit{current_period}"

        if entry_key not in attendance_data:
            print(f"{entry_key} not found => This student did not enter in period {current_period}. Skipping.")
            continue

        # entry はある
        # exit がない => "○"
        # exit もある => decision を読み取る
        if exit_key not in attendance_data:
            status = "○"  # 在室中
        else:
            # ---- 6. course_id 配列から該当 period を持つ decision を取得 ----
            #     attendance_data["course_id"] は配列または辞書になっている想定
            course_id_array = attendance_data.get("course_id")
            decision = None
            if isinstance(course_id_array, list):
                # 各要素に { "decision": "...", "period": 数字 } が入っている想定
                for elem in course_id_array:
                    if not elem:
                        continue
                    if elem.get("period") == current_period:
                        decision = elem.get("decision")
                        break

            status = decision if decision else "?"  # 該当が無い場合は "?" など

        # ---- 7. シートの書き込み ----
        row, column = map_row_column(current_day_of_month, idx, current_period)
        print(f" -> Updating row={row}, col={column} with status='{status}'")
        try:
            sheet.update_cell(row, column, status)
        except Exception as e:
            print(f"Error updating cell: {e}")

    print("Done.")

if __name__ == "__main__":
    main()