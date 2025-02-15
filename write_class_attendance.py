import datetime
from zoneinfo import ZoneInfo  # Python3.9以降なら標準ライブラリでOK
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ---------------------
# Firebase & GSpread 初期化
# ---------------------
if not firebase_admin._apps:
    cred = credentials.Certificate("/tmp/firebase_service_account.json")
    firebase_admin.initialize_app(
        cred, {"databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/"}
    )
    print(f"[Debug] Firebase initialized.")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/gcp_service_account.json", scope)
gclient = gspread.authorize(creds)
print(f"[Debug] Google Sheets API authorized.")


# ---------------------
# Firebase アクセス関数
# ---------------------
def get_data_from_firebase(path):
    """
    指定したRealtime Databaseパスからデータを取得する。
    """
    print(f"[Debug] Fetching data from Firebase path: {path}")
    ref = db.reference(path)
    data = ref.get()
    if data is None:
        print(f"[Debug] No data found at path: {path}")
    return data


# ---------------------
# ヘルパー関数
# ---------------------
def get_current_date_details():
    """
    実行時の日時を【日本時間 (JST)】で取得し、曜日・シート名・日付(数値)を返す。
    (Python3.9以降で利用可能な zoneinfo を使用)
    """
    now = datetime.datetime.now(ZoneInfo("Asia/Tokyo"))

    current_day = now.strftime("%A")            # 例: "Sunday"
    current_sheet_name = now.strftime("%Y-%m")  # 例: "2025-01"
    current_day_of_month = now.day             # 例: 26
    return now, current_day, current_sheet_name, current_day_of_month


def map_date_period_to_column(day_of_month, period):
    """
    列番号 = (日付*4) + period - 2
    """
    return (day_of_month * 4) + period - 2


def parse_student_indices(student_indices_str):
    """ "E523, E534" のような文字列をリストに変換 """
    return [s.strip() for s in student_indices_str.split(",")]


def parse_course_ids(course_ids_str):
    """ "1, 2" のような文字列をリストに変換（数値化もする） """
    ids = [s.strip() for s in course_ids_str.split(",")]
    return [int(i) for i in ids if i.isdigit()]


def get_period_from_now(now):
    """
    現在時刻がどのperiodに該当するかを判定して返す。
    以下は固定した例です。要件に合わせて修正可。
      period1: 08:50-10:20
      period2: 10:30-12:00
      period3: 13:10-14:40
      period4: 14:50-16:20
    該当なしの場合は None を返す。
    """
    def hm_to_dt(hh, mm):
        return now.replace(hour=hh, minute=mm, second=0, microsecond=0)

    time_ranges = {
        1: (hm_to_dt(8, 50), hm_to_dt(10, 20)),
        2: (hm_to_dt(10, 30), hm_to_dt(12, 0)),
        3: (hm_to_dt(13, 10), hm_to_dt(14, 40)),
        4: (hm_to_dt(14, 50), hm_to_dt(16, 20)),
    }

    for p, (start, end) in time_ranges.items():
        if start <= now <= end:
            return p
    return None


def find_course_id_by_period(possible_course_ids, target_period):
    """
    与えられた course_id リストのうち、Courses の schedule.period == target_period
    となる course_id を一つ返す。
    存在しない場合は None
    """
    for cid in possible_course_ids:
        course_info = get_data_from_firebase(f"Courses/course_id/{cid}")
        if course_info is None:
            continue
        course_schedule = course_info.get("schedule", {})
        if course_schedule.get("period") == target_period:
            return cid
    return None


# ---------------------
# メイン処理
# ---------------------
def main(class_index="E5"):
    # 1. 現在日時の取得 (日本時間)
    now, current_day, current_sheet_name, current_day_of_month = get_current_date_details()
    print(f"[Debug] Now (JST): {now}")
    print(f"[Debug] Current day: {current_day}")
    print(f"[Debug] Current sheet name: {current_sheet_name}")
    print(f"[Debug] Current day of month: {current_day_of_month}")

    # 2. Class データ取得
    class_data_path = f"Class/class_index/{class_index}"
    class_data = get_data_from_firebase(class_data_path)
    if not class_data:
        print(f"[Debug] No data found for class_index: {class_index}")
        return

    class_sheet_id = class_data.get("class_sheet_id")
    if not class_sheet_id:
        print(f"[Debug] No class_sheet_id found under Class/class_index/{class_index}")
        return

    # 例: "1, 2"
    course_ids_str = class_data.get("course_id", "")
    if not course_ids_str:
        print(f"[Debug] No course_id info under Class/class_index/{class_index}")
        return
    possible_course_ids = parse_course_ids(course_ids_str)

    # 例: "E523, E534"
    student_indices_str = class_data.get("student_index", "")
    if not student_indices_str:
        print(f"[Debug] No student_index info under Class/class_index/{class_index}")
        return
    student_indices = parse_student_indices(student_indices_str)
    print(f"[Debug] Target class_sheet_id: {class_sheet_id}")
    print(f"[Debug] Possible course_ids: {possible_course_ids}")
    print(f"[Debug] Student indices: {student_indices}")

    # 3. 現在の時刻がどのperiodに該当するか判定
    period = get_period_from_now(now)
    if period is None:
        print(f"[Debug] 現在の時刻はどの授業時間にも該当しないため、処理をスキップします。")
        return
    print(f"[Debug] Current time corresponds to period: {period}")

    # 4. period に合致するコースを特定
    target_course_id = find_course_id_by_period(possible_course_ids, period)
    if target_course_id is None:
        print(f"[Debug] Class {class_index} に紐づくコースの中で period={period} のコースが見つかりません。スキップします。")
        return
    print(f"[Debug] Target course_id for period {period} is: {target_course_id}")

    # 5. Google シートを開いて該当ワークシートを取得
    try:
        sh = gclient.open_by_key(class_sheet_id)
        print(f"[Debug] Opened Google Sheet: {sh.title}")
        try:
            sheet = sh.worksheet(current_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"[Debug] Worksheet '{current_sheet_name}' not found in spreadsheet {class_sheet_id}.")
            return
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"[Debug] Spreadsheet with ID {class_sheet_id} not found.")
        return
    print(f"[Debug] Using worksheet: {sheet.title}")

    # 6. 各 student_index について attendance をチェック
    for idx, student_idx in enumerate(student_indices, start=1):
        row_number = idx + 2  # ヘッダーが1行目にある想定 → 受講生1人目は 2行目
        print(f"[Debug]\nProcessing student_index: {student_idx} (row={row_number})")

        # student_id を取得
        student_id_path = f"Students/student_info/student_index/{student_idx}/student_id"
        student_id = get_data_from_firebase(student_id_path)
        if not student_id:
            print(f"[Debug] No student_id found for student_index {student_idx}. Skipping.")
            continue
        print(f"[Debug] Found student_id: {student_id}")

        # attendance データ
        attendance_path = f"Students/attendance/student_id/{student_id}"
        attendance_data = get_data_from_firebase(attendance_path)
        if not attendance_data:
            print(f"[Debug] No attendance data for student_id {student_id}. Skipping.")
            continue

        # entry{period} と exit{period} を確認
        entry_key = "entry1"
        exit_key = "exit1"

        if entry_key not in attendance_data:
            print(f"[Debug] No {entry_key} found ⇒ skip.")
            continue  # entry1が無い場合はスキップ

        # entry1 があった場合
        if exit_key not in attendance_data:
            # exit1 が無い ⇒ entry1["read_datetime"] を '%Y-%m' 形式でステータスに
            entry_time_str = attendance_data[entry_key].get("read_datetime")
            if not entry_time_str:
                print(f"[Debug] {entry_key} exists but no 'read_datetime' ⇒ skip.")
                continue

            # "2025-01-26 09:00:50" のような文字列をパース
            try:
                dt_obj = datetime.datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
                status = dt_obj.strftime("%H-%M")
            except ValueError:
                # パース失敗時は原文そのままとする
                status = entry_time_str
            print(f"[Debug] entry1 found but exit1 not found ⇒ status='{status}'")

        else:
            # entry1 と exit1 が両方ある ⇒ decision を取得
            decision_path = f"Students/attendance/student_id/{student_id}/course_id/{target_course_id}/decision"
            decision = get_data_from_firebase(decision_path)
            if decision is None:
                print(f"[Debug] No decision found at {decision_path}. Defaulting to '×'.")
                status = "×"
            else:
                status = decision
            print(f"[Debug] entry1 and exit1 found ⇒ decision='{status}'")

        # 7. シートに書き込み
        column_number = map_date_period_to_column(current_day_of_month, period)
        try:
            sheet.update_cell(row_number, column_number, status)
            print(f"[Debug] Updated cell(row={row_number}, col={column_number}) with '{status}'.")
        except Exception as e:
            print(f"[Debug] Error updating sheet: {e}")


if __name__ == "__main__":
    # 例として class_index="E5" を指定して実行
    main("E5")
