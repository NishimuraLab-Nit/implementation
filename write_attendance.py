import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------
# Firebase & GSpread初期化
# ---------------------
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gclient = gspread.authorize(creds)

# ---------------------
# Firebaseアクセス関連
# ---------------------
def get_data_from_firebase(path):
    ref = db.reference(path)
    return ref.get()

def update_data_in_firebase(path, data_dict):
    ref = db.reference(path)
    ref.update(data_dict)

# ---------------------
# ユーティリティ
# ---------------------
def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    """文字列→datetimeに変換。失敗時はNone"""
    try:
        return datetime.datetime.strptime(dt_str, fmt)
    except Exception:
        return None

def parse_hhmm_range(range_str):
    """
    "8:50~10:20" のような文字列を
    (datetime.time(8,50), datetime.time(10,20)) に変換して返す。
    パースに失敗したら (None, None) を返す。
    """
    try:
        start_str, end_str = range_str.split("~")
        sh, sm = map(int, start_str.split(":"))
        eh, em = map(int, end_str.split(":"))
        return datetime.time(sh, sm, 0), datetime.time(eh, em, 0)
    except:
        return None, None

def combine_date_and_time(date_dt, time_obj):
    """
    date_dt (datetime.date or datetime.datetime で日付部分を使用)
    +
    time_obj (datetime.time)
    =
    datetime.datetime (同じ日付＋指定の時刻)
    """
    return datetime.datetime(
        date_dt.year,
        date_dt.month,
        date_dt.day,
        time_obj.hour,
        time_obj.minute,
        time_obj.second
    )

# ---------------------
# 出欠判定ロジック
# ---------------------
def judge_attendance_for_course(entry_dt, exit_dt, start_dt, finish_dt):
    """
    仕様に沿った判定:
      - 欠席(×):
        entry_dt >= finish_dt
      - 早退(△早):
        exit_dt が存在 & entry_dt <= start+5分 & exit_dt < finish-5分
      - 遅刻(△遅):
        exit_dt が存在 & entry_dt > start+5分 & exit_dt <= finish+5分
      - 正常(○):
        1) entry_dt <= start+5分 & exit_dt <= finish+5分
        2) exit_dt > finish+5分 → exitをfinishに補正, 次コマ entry=finish+10分
        3) exit_dt is None → exitをfinishに補正 (正常扱い)
      - それ以外は "？" とする

    戻り値:
      ( status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt )
    """
    status_str = ""
    updated_entry_dt = entry_dt
    updated_exit_dt = exit_dt
    next_course_entry_dt = None

    td_5min  = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    # (1) 欠席判定
    if entry_dt >= finish_dt:
        return "×", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # --- ここから exit_dt が None の場合を先に判定 ---
    if exit_dt is None:
        # 仕様例: exitが無い場合は正常(○)扱い & exitを finish_dt で保存
        status_str = "○"
        updated_exit_dt = finish_dt
        return status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # --- exit_dt is not None なので比較可能 ---

    # (2) 早退(△早)
    #     entry_dt <= (start_dt + 5分) and exit_dt < (finish_dt - 5分)
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt < (finish_dt - td_5min)):
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        return f"△早{delta_min}分", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # (3) 遅刻(△遅)
    #     entry_dt > (start_dt + 5分) and exit_dt <= (finish_dt + 5分)
    if (entry_dt > (start_dt + td_5min)) and (exit_dt <= (finish_dt + td_5min)):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # (4) 正常(○)
    #     1) entry_dt <= start+5分 and exit_dt <= finish+5分
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt <= (finish_dt + td_5min)):
        return "○", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    #     2) exit_dt > finish+5分
    if exit_dt > (finish_dt + td_5min):
        status_str = "○"
        updated_exit_dt = finish_dt
        # 次コマentry = finish+10分  (日付固定したいならここで強制する)
        temp_dt = finish_dt + td_10min
        forced_next_dt = datetime.datetime(
            finish_dt.year, finish_dt.month, finish_dt.day,
            temp_dt.hour, temp_dt.minute, temp_dt.second
        )
        next_course_entry_dt = forced_next_dt
        return status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # 上記以外 → "？"
    return "？", updated_entry_dt, updated_exit_dt, next_course_entry_dt

# ---------------------
# メインフロー
# ---------------------
def process_attendance_and_write_sheet():
    """
    1) Students/attendance/student_id/ の各student_idで entry/exitを取得
    2) student_info から student_index を取得
    3) enrollment(Students/enrollment/student_index/xxx) で受講コースID取得
    4) コースschedule と entry/exit を比較 → 出欠判定
    5) 結果をシートに書き込み(シート名= "YYYY-MM", 列= 日+1, 行= コースID+1)
       ここでは「同日の授業」を想定し、日付列がズレないよう base_date を固定
    """

    # Students/attendance
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("attendanceデータがありません。終了します。")
        return

    # Courses の全体 (0番目: None想定)
    courses_data = get_data_from_firebase("Courses/course_id")
    # student_info
    student_info_data = get_data_from_firebase("Students/student_info")

    # 書き込み用辞書: (student_index, course_id, yyyymmdd) -> status
    results_dict = {}

    # -----------------
    # 受講生ごとにループ
    # -----------------
    for student_id, attendance_dict in attendance_data.items():
        if not isinstance(attendance_dict, dict):
            continue

        # student_index 取得
        student_index = None
        if (student_info_data.get("student_id") and
            student_id in student_info_data["student_id"] and
            "student_index" in student_info_data["student_id"][student_id]):
            student_index = student_info_data["student_id"][student_id]["student_index"]
        if not student_index:
            continue

        # enrollment
        enroll_path = f"Students/enrollment/student_index/{student_index}"
        enrollment_data = get_data_from_firebase(enroll_path)
        if not enrollment_data or "course_id" not in enrollment_data:
            continue

        course_id_str = enrollment_data["course_id"]  # 例: "1, 2"
        course_id_list = [x.strip() for x in course_id_str.split(",") if x.strip()]
        if not course_id_list:
            continue

        # attendance_dict の entry/exit をリスト化
        # 例: [('entry1','exit1'), ('entry2','exit2'), ...]
        entry_exit_pairs = []
        i = 1
        while True:
            ekey = f"entry{i}"
            xkey = f"exit{i}"
            if ekey not in attendance_dict:
                break
            entry_exit_pairs.append((ekey, xkey))
            i += 1

        if not entry_exit_pairs:
            # entryが一つも無い場合 → 全コース欠席扱い or スキップ(仕様次第)
            # ここではとりあえずスキップ
            continue

        # -----------------------------------
        # 1) base_date の設定
        #    最初のentry_dt から日付だけ取得し固定化する
        # -----------------------------------
        first_entry_key, _ = entry_exit_pairs[0]
        first_entry_dt_str = attendance_dict[first_entry_key].get("read_datetime", "")
        first_entry_dt = parse_datetime(first_entry_dt_str) if first_entry_dt_str else None
        if not first_entry_dt:
            # 最初のエントリーがない → スキップ(または欠席扱い)
            continue

        # base_date: datetime.date (YYYY-MM-DD)
        base_date = first_entry_dt.date()  # 例: 2025-01-06

        pair_index = 0

        # -----------------
        # コースごとのループ
        # -----------------
        for c_id_str in course_id_list:
            try:
                c_id = int(c_id_str)
            except:
                continue
            if c_id <= 0 or c_id >= len(courses_data):
                continue

            course_info = courses_data[c_id]
            schedule_info = course_info.get("schedule", {})
            time_range_str = schedule_info.get("time", "")  # 例: "8:50~10:20"
            if not time_range_str:
                continue

            # ペア不足 → 欠席
            if pair_index >= len(entry_exit_pairs):
                absent_date_str = base_date.strftime("%Y-%m-%d")
                results_dict[(student_index, c_id, absent_date_str)] = "×"
                continue

            # エントリー/エグジット取り出し
            ekey, xkey = entry_exit_pairs[pair_index]
            pair_index += 1

            entry_rec = attendance_dict.get(ekey, {})
            exit_rec  = attendance_dict.get(xkey, {})
            entry_dt_str = entry_rec.get("read_datetime")
            exit_dt_str  = exit_rec.get("read_datetime")
            entry_dt = parse_datetime(entry_dt_str) if entry_dt_str else None
            exit_dt  = parse_datetime(exit_dt_str) if exit_dt_str else None

            if not entry_dt:
                # entry無 → 欠席扱い
                absent_date_str = base_date.strftime("%Y-%m-%d")
                results_dict[(student_index, c_id, absent_date_str)] = "×"
                continue

            # コース開始/終了を base_date + HH:MM で作成
            start_t, finish_t = parse_hhmm_range(time_range_str)
            if not start_t or not finish_t:
                continue

            start_dt = combine_date_and_time(base_date, start_t)
            finish_dt = combine_date_and_time(base_date, finish_t)

            # 出欠判定
            status, new_entry_dt, new_exit_dt, next_course_entry_dt = judge_attendance_for_course(
                entry_dt, exit_dt, start_dt, finish_dt
            )

            # Firebase更新 (entry/exit)
            updates = {}
            # entry更新
            if new_entry_dt and new_entry_dt != entry_dt:
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_rec.get("serial_number", "")
                }
            # exit更新
            if new_exit_dt and new_exit_dt != exit_dt:
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_rec.get("serial_number", "")
                }

            # 次コマentry作成
            if next_course_entry_dt:
                next_ekey = f"entry{pair_index+1}"
                next_xkey = f"exit{pair_index+1}"
                updates[next_ekey] = {
                    "read_datetime": next_course_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_rec.get("serial_number", "")
                }
                if exit_dt:
                    updates[next_xkey] = {
                        "read_datetime": exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": exit_rec.get("serial_number", "")
                    }

            if updates:
                att_path = f"Students/attendance/student_id/{student_id}"
                update_data_in_firebase(att_path, updates)

            # シート書き込み用に結果を格納
            # シート日付は base_date 固定
            sheet_date_str = base_date.strftime("%Y-%m-%d")
            results_dict[(student_index, c_id, sheet_date_str)] = status

    # -----------------
    # シート書き込み
    # -----------------
    all_student_index_data = student_info_data.get("student_index", {})
    for std_idx, info_val in all_student_index_data.items():
        sheet_id = info_val.get("sheet_id")
        if not sheet_id:
            continue

        try:
            sh = gclient.open_by_key(sheet_id)
        except Exception as e:
            print(f"シート({sheet_id}) を開けません: {e}")
            continue

        # 当該 student_index の結果のみ取得
        std_result_items = {
            k: v for k, v in results_dict.items() if k[0] == std_idx
        }
        if not std_result_items:
            continue

        # 書き込み
        for (s_idx, c_id, date_str), status_val in std_result_items.items():
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # %d

            # Worksheet
            try:
                ws = sh.worksheet(yyyymm)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            row = c_id + 1  # コースIDが1始まりの想定
            col = day + 1   # 日付(%d) +1
            try:
                ws.update_cell(row, col, status_val)
            except Exception as e:
                print(f"シート書き込み失敗 [{sheet_id}:{yyyymm} R{row}C{col}]: {e}")

    print("=== 出席判定とシート更新が完了しました ===")

# ---------------------
# 実行セクション
# ---------------------
if __name__ == "__main__":
    process_attendance_and_write_sheet()
