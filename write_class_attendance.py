import datetime
from zoneinfo import ZoneInfo
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
    print("[Debug] Firebase initialized.")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/gcp_service_account.json", scope)
gclient = gspread.authorize(creds)
print("[Debug] Google Sheets API authorized.")


def get_data_from_firebase(path):
    """
    指定パスからFirebaseのデータを取得します。
    """
    print(f"[Debug] Fetching data from Firebase path: {path}")
    ref = db.reference(path)
    data = ref.get()
    if data is None:
        print(f"[Debug] No data found at path: {path}")
    return data


def get_current_date_details():
    """
    日本時間 (JST) の現在時刻を取得し、曜日・シート名・日付を返します。
    """
    now = datetime.datetime.now(ZoneInfo("Asia/Tokyo"))
    current_day = now.strftime("%A")
    current_sheet_name = now.strftime("%Y-%m")
    current_day_of_month = now.day
    return now, current_day, current_sheet_name, current_day_of_month


def map_date_period_to_column(day_of_month, period):
    """
    列番号を日付とコマ数から計算して返します。
    例: 列番号 = (日付 * 4) + period - 2
    """
    return (day_of_month * 4) + period - 2


def parse_student_indices(student_indices_str):
    """
    "E523, E534" のような文字列をリスト化します。
    """
    return [s.strip() for s in student_indices_str.split(",")]


def parse_course_ids(course_ids_str):
    """
    "1, 2" のような文字列をリスト化し、数値に変換して返します。
    """
    ids = [s.strip() for s in course_ids_str.split(",")]
    return [int(i) for i in ids if i.isdigit()]


def get_period_from_now(now):
    """
    現在時刻がどのperiodに該当するかを判定して返します。該当しない場合は None。
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


def process_single_class(class_index, now, current_day, current_sheet_name, current_day_of_month):
    """
    1つのクラスを処理する。  
    指定クラスのスプレッドシートを開き、現在の時刻に該当する period のコース列に出席情報を記載します。
    """
    print(f"\n[Debug] ========== Start processing class_index: {class_index} ==========")
    # Classデータ取得（パスを統一）
    class_data_path = f"Classes/class_index/{class_index}"
    class_data = get_data_from_firebase(class_data_path)
    if not class_data:
        print(f"[Debug] No data found for class_index: {class_index}")
        return

    class_sheet_id = class_data.get("class_sheet_id")
    if not class_sheet_id:
        print(f"[Debug] No class_sheet_id found under {class_data_path}")
        return

    course_ids_str = class_data.get("course_id", "")
    if not course_ids_str:
        print(f"[Debug] No course_id info under {class_data_path}")
        return
    possible_course_ids = parse_course_ids(course_ids_str)

    student_indices_str = class_data.get("student_index", "")
    if not student_indices_str:
        print(f"[Debug] No student_index info under {class_data_path}")
        return
    student_indices = parse_student_indices(student_indices_str)

    print(f"[Debug] Target class_sheet_id: {class_sheet_id}")
    print(f"[Debug] Possible course_ids: {possible_course_ids}")
    print(f"[Debug] Student indices: {student_indices}")

    # period判定
    period = get_period_from_now(now)
    if period is None:
        print("[Debug] 現在の時刻はどの授業時間にも該当しません。処理をスキップします。")
        return
    print(f"[Debug] Current time corresponds to period: {period}")

    # Googleシートを開き、ワークシートを取得
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

    # 学生ごとの attendance をチェック
    for idx, student_idx in enumerate(student_indices, start=1):
        row_number = idx + 2
        print(f"[Debug]\nProcessing student_index: {student_idx} (row={row_number})")

        student_id_path = f"Students/student_info/student_index/{student_idx}/student_id"
        student_id = get_data_from_firebase(student_id_path)
        if not student_id:
            print(f"[Debug] No student_id found for student_index {student_idx}. Skipping.")
            continue
        print(f"[Debug] Found student_id: {student_id}")

        attendance_path = f"Students/attendance/student_id/{student_id}"
        attendance_data = get_data_from_firebase(attendance_path)
        if not attendance_data:
            print(f"[Debug] No attendance data for student_id {student_id}. Skipping.")
            continue

        entry_key = "entry1"
        exit_key = "exit1"

        # entryがない場合はスキップ
        if entry_key not in attendance_data:
            print(f"[Debug] No {entry_key} found ⇒ skip.")
            continue

        # entryのみの場合は「○」とする
        if exit_key not in attendance_data:
            status = "○"
            print(f"[Debug] entry1 found but exit1 not found ⇒ status='{status}'")
        else:
            # entry, exit両方がある場合、全てのcourse_idのdecisionを取得する
            decisions = []
            for cid in possible_course_ids:
                decision_path = f"Students/attendance/student_id/{student_id}/course_id/{cid}/decision"
                decision = get_data_from_firebase(decision_path)
                if decision is None:
                    decision = "×"
                decisions.append(str(decision))
            status = ", ".join(decisions)
            print(f"[Debug] entry1 and exit1 found ⇒ decisions='{status}'")

        column_number = map_date_period_to_column(current_day_of_month, period)
        try:
            sheet.update_cell(row_number, column_number, status)
            print(f"[Debug] Updated cell(row={row_number}, col={column_number}) with '{status}'.")
        except Exception as e:
            print(f"[Debug] Error updating sheet: {e}")


def main():
    """
    全クラスをループし、共通処理をまとめて実行する。
    """
    # 日付や現在時刻に関する情報を先に取得
    now, current_day, current_sheet_name, current_day_of_month = get_current_date_details()
    print(f"[Debug] Now (JST): {now}")
    print(f"[Debug] Current day: {current_day}")
    print(f"[Debug] Current sheet name: {current_sheet_name}")
    print(f"[Debug] Current day of month: {current_day_of_month}")

    # Firebase の "Classes/class_index" から全クラス情報を一括取得（パスを統一）
    all_classes_data = get_data_from_firebase("Classes/class_index")
    if not all_classes_data:
        print("[Debug] No class data found at 'Classes/class_index'.")
        return

    # 取得したクラス一覧をループし、1クラスずつ処理
    for class_index in all_classes_data.keys():
        process_single_class(
            class_index,
            now,
            current_day,
            current_sheet_name,
            current_day_of_month
        )


if __name__ == "__main__":
    main()
