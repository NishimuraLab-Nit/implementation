import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pytz

# ---------------------
# Firebase & GSpread 初期化
# ---------------------
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })
    print("Firebase initialized.")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gclient = gspread.authorize(creds)
print("Google Sheets API authorized.")


# ---------------------
# Firebase アクセス関数
# ---------------------
def get_data_from_firebase(path):
    """
    指定したRealtime Databaseパスからデータを取得する。
    """
    print(f"Fetching data from Firebase path: {path}")
    ref = db.reference(path)
    data = ref.get()
    if data is None:
        print(f"No data found at path: {path}")
    return data

# ---------------------
# ヘルパー関数
# ---------------------
def get_current_date_details():
    """
    実行時の日時を取得し、曜日・シート名・日付(数値)を返す。
    """
    now = datetime.datetime.now()
    current_day = now.strftime('%A')           # 例: "Sunday"
    current_sheet_name = now.strftime('%Y-%m') # 例: "2025-01"
    current_day_of_month = now.day            # 例: 26
    return now, current_day, current_sheet_name, current_day_of_month

def get_current_date_details():
    # 日本時間を指定
    jst = pytz.timezone("Asia/Tokyo")
    now = datetime.datetime.now(jst)
    
    current_day = now.strftime('%A')           # 例: "Sunday"
    current_sheet_name = now.strftime('%Y-%m') # 例: "2025-01"
    current_day_of_month = now.day            # 例: 26
    
    return now, current_day, current_sheet_name, current_day_of_month


def map_date_period_to_column(day_of_month, period):
    """
    列番号 = (日付*4) + period - 2
    
    例) 日付=1, period=1 ⇒ 列=3
        日付=26, period=3 ⇒ 列 = (26*4)+3-2 = 105
    """
    return (day_of_month * 4) + period - 2

def parse_student_indices(student_indices_str):
    """
    "E523, E534" のような文字列をリストに変換
    """
    return [s.strip() for s in student_indices_str.split(',')]

def parse_course_ids(course_ids_str):
    """
    "1, 2" のような文字列をリストに変換（数値化もする）
    """
    ids = [s.strip() for s in course_ids_str.split(',')]
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
    # 当日の年月日を維持して、時刻だけ上書き
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
        course_schedule = course_info.get('schedule', {})
        if course_schedule.get('period') == target_period:
            return cid
    return None


# ---------------------
# メイン処理
# ---------------------
def main(class_index="E5"):
    # 1. 現在日時の取得
    now, current_day, current_sheet_name, current_day_of_month = get_current_date_details()
    print(f"Now: {now}")
    print(f"Current day: {current_day}")
    print(f"Current sheet name: {current_sheet_name}")
    print(f"Current day of month: {current_day_of_month}")

    # 2. Class データ取得（class_sheet_id, course_id, student_index など）
    class_data_path = f"Class/class_index/{class_index}"
    class_data = get_data_from_firebase(class_data_path)
    if not class_data:
        print(f"No data found for class_index: {class_index}")
        return

    class_sheet_id = class_data.get('class_sheet_id')
    if not class_sheet_id:
        print(f"No class_sheet_id found under Class/class_index/{class_index}")
        return

    # 例: "1, 2"
    course_ids_str = class_data.get('course_id', "")
    if not course_ids_str:
        print(f"No course_id info under Class/class_index/{class_index}")
        return
    possible_course_ids = parse_course_ids(course_ids_str)

    # 例: "E523, E534"
    student_indices_str = class_data.get('student_index', "")
    if not student_indices_str:
        print(f"No student_index info under Class/class_index/{class_index}")
        return
    student_indices = parse_student_indices(student_indices_str)
    print(f"Target class_sheet_id: {class_sheet_id}")
    print(f"Possible course_ids: {possible_course_ids}")
    print(f"Student indices: {student_indices}")

    # 3. 現在の時刻がどの period に該当するか判定
    period = get_period_from_now(now)
    if period is None:
        print("現在の時刻はどの授業時間にも該当しないため、処理をスキップします。")
        return
    print(f"Current time corresponds to period: {period}")

    # 4. period に合致するコースを特定
    target_course_id = find_course_id_by_period(possible_course_ids, period)
    if target_course_id is None:
        print(f"Class {class_index} に紐づくコースの中で period={period} のコースが見つかりません。スキップします。")
        return
    print(f"Target course_id for period {period} is: {target_course_id}")

    # 5. Google シートを開いて該当ワークシートを取得
    try:
        sh = gclient.open_by_key(class_sheet_id)
        print(f"Opened Google Sheet: {sh.title}")
        try:
            sheet = sh.worksheet(current_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{current_sheet_name}' not found in spreadsheet {class_sheet_id}.")
            return
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Spreadsheet with ID {class_sheet_id} not found.")
        return
    print(f"Using worksheet: {sheet.title}")

    # 6. 各 student_index について attendance をチェック
    for idx, student_idx in enumerate(student_indices, start=1):
        row_number = idx + 2  # ヘッダーが1行目にある想定 → 受講生1人目は 2行目
        print(f"\nProcessing student_index: {student_idx} (row={row_number})")

        # student_id を取得
        student_id_path = f"Students/student_info/student_index/{student_idx}/student_id"
        student_id = get_data_from_firebase(student_id_path)
        if not student_id:
            print(f"No student_id found for student_index {student_idx}. Skipping.")
            continue
        print(f"Found student_id: {student_id}")

        # attendance データ
        attendance_path = f"Students/attendance/student_id/{student_id}"
        attendance_data = get_data_from_firebase(attendance_path)
        if not attendance_data:
            print(f"No attendance data for student_id {student_id}. Skipping.")
            continue

        # entry{period} と exit{period} を確認
        entry_key = f"entry{period}"
        exit_key  = f"exit{period}"

        if entry_key not in attendance_data:
            print(f"No {entry_key} found ⇒ skip.")
            continue  # entryが無い場合はスキップ

        # entry{period} があった場合
        if exit_key not in attendance_data:
            # exitが無い ⇒ ステータス "◯"
            status = "◯"
            print(f"entry{period} found but exit{period} not found ⇒ status='{status}'")
        else:
            # entry と exit 両方ある ⇒ decision を取得
            decision_path = f"Students/attendance/student_id/{student_id}/course_id/{target_course_id}/decision"
            decision = get_data_from_firebase(decision_path)
            if decision is None:
                print(f"No decision found at {decision_path}. Defaulting to '×' or skipping.")
                status = "×"  # ここは運用次第で変えてください
            else:
                status = decision
            print(f"entry{period} and exit{period} found ⇒ decision='{status}'")

        # 7. シートに書き込み
        column_number = map_date_period_to_column(current_day_of_month, period)
        try:
            sheet.update_cell(row_number, column_number, status)
            print(f"Updated cell(row={row_number}, col={column_number}) with '{status}'.")
        except Exception as e:
            print(f"Error updating sheet: {e}")

if __name__ == "__main__":
    # 例として class_index="E5" を指定
    main("E5")
