import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ===============================
# Firebase・GSpread初期化
# ===============================
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gspread_client = gspread.authorize(creds)

# ===============================
# Firebaseヘルパー関数
# ===============================
def get_data_from_firebase(path):
    ref = db.reference(path)
    return ref.get()

def update_data_in_firebase(path, data):
    ref = db.reference(path)
    ref.update(data)

# ===============================
# パース系ヘルパー
# ===============================
def parse_datetime(dt_str):
    return datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")

def parse_time_range(time_str):
    start_str, end_str = time_str.split("~")
    start_h, start_m = map(int, start_str.split(":"))
    end_h, end_m = map(int, end_str.split(":"))
    return start_h, start_m, end_h, end_m

def combine_date_time(base_dt, hour, minute):
    return datetime.datetime(base_dt.year, base_dt.month, base_dt.day, hour, minute)

# ===============================
# 出欠判定ロジック (条件②③修正済み)
# ===============================
def judge_attendance(entry_dt, exit_dt, start_dt, finish_dt):
    delta_5min = datetime.timedelta(minutes=5)
    delta_10min = datetime.timedelta(minutes=10)

    if entry_dt is None:
        return ("✕", None, None, None, "")

    # (②)
    if exit_dt is not None and exit_dt >= finish_dt + delta_5min and entry_dt <= start_dt + delta_5min:
        old_exit = exit_dt
        fix_exit_cur = finish_dt
        fix_entry_next = finish_dt + delta_10min
        fix_exit_next = old_exit
        return ("〇", fix_entry_next, fix_exit_cur, fix_exit_next, "")

    # (③)
    if exit_dt is None and entry_dt <= start_dt + delta_5min:
        fix_exit_cur = finish_dt
        fix_entry_next = finish_dt + delta_10min
        fix_exit_next = None
        return ("〇", fix_entry_next, fix_exit_cur, fix_exit_next, "")

    # 早退 (△早)
    if (entry_dt <= start_dt + delta_5min) and (exit_dt is not None) and (exit_dt <= finish_dt - delta_5min):
        diff_min = int((finish_dt - exit_dt).total_seconds() // 60)
        note_str = f"△早{diff_min}分"
        return ("△早", None, None, None, note_str)

    # 遅刻 (△遅)
    if (entry_dt > start_dt + delta_5min) and (exit_dt is not None) and (exit_dt <= finish_dt + delta_5min):
        diff_min = int((entry_dt - start_dt).total_seconds() // 60)
        note_str = f"△遅{diff_min}分"
        return ("△遅", None, None, None, note_str)

    # 正常出席 (〇)
    if (entry_dt <= start_dt + delta_5min) and (exit_dt is not None) and (exit_dt <= finish_dt + delta_5min):
        return ("〇", None, None, None, "")

    # どれにも該当しなければ欠席
    return ("✕", None, None, None, "")

# ===============================
# メイン処理
# ===============================
def main_process():
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("No attendance data.")
        return

    courses_data = get_data_from_firebase("Courses/course_id")

    attendance_result_dict = {}
    firebase_updates = []

    for student_id, entries_exits_dict in attendance_data.items():
        if not entries_exits_dict:
            continue

        # student_info -> student_index
        info_path = f"Students/student_info/student_id/{student_id}"
        student_info = get_data_from_firebase(info_path)
        if not student_info or "student_index" not in student_info:
            continue
        student_index = student_info["student_index"]

        # enrollment -> course_id
        enroll_path = f"Students/enrollment/student_index/{student_index}"
        enroll_info = get_data_from_firebase(enroll_path)
        if not enroll_info or "course_id" not in enroll_info:
            continue
        course_id_str = enroll_info["course_id"]  # "1, 2" など
        course_ids = [cid.strip() for cid in course_id_str.split(",") if cid.strip()]

        # ---------------------------
        # 【修正】エントリとエグジットを番号別にまとめる
        # ---------------------------
        entry_dict = {}
        exit_dict = {}

        # キーをソート( entry1, entry2, exit1, exit2 ... の順 )
        sorted_keys = sorted(entries_exits_dict.keys())

        for k in sorted_keys:
            v = entries_exits_dict[k]
            if not isinstance(v, dict):
                continue

            dt_str = v.get("read_datetime")
            if not dt_str:
                continue

            if k.startswith("entry"):
                idx_num = k.replace("entry", "")
                entry_dict[idx_num] = parse_datetime(dt_str)
            elif k.startswith("exit"):
                idx_num = k.replace("exit", "")
                exit_dict[idx_num] = parse_datetime(dt_str)

        # 上で entry_dict={ '1': datetime(...), '2': ... }, exit_dict={ '1': ..., '2': ... } のように番号ごとに格納
        # すべての番号をまとめてソート
        all_indices = sorted(set(entry_dict.keys()) | set(exit_dict.keys()))

        # ペアにする
        entry_exit_pairs = []
        for i in all_indices:
            e_dt = entry_dict.get(i, None)
            x_dt = exit_dict.get(i, None)
            entry_exit_pairs.append((i, e_dt, x_dt))

        # ---------------------------
        # コースIDぶん(順番に)処理
        # ---------------------------
        pair_idx = 0
        for i, course_id in enumerate(course_ids, start=1):
            try:
                int_course_id = int(course_id)
            except ValueError:
                continue

            if not courses_data or int_course_id >= len(courses_data) or courses_data[int_course_id] is None:
                continue

            course_info = courses_data[int_course_id]
            schedule_info = course_info.get("schedule", {})
            time_str = schedule_info.get("time")
            if not time_str:
                continue

            # ペアがもう足りなければ欠席扱い
            if pair_idx >= len(entry_exit_pairs):
                date_str = datetime.datetime.now().strftime("%Y-%m-%d")
                attendance_result_dict[(student_index, date_str, int_course_id)] = "✕"
                continue

            idx_str, entry_dt, exit_dt = entry_exit_pairs[pair_idx]
            # 日付取得
            # ここでは entry_dt がある前提でそれを使う(なければ現在日付でも可)
            if entry_dt:
                date_str = entry_dt.strftime("%Y-%m-%d")
            else:
                date_str = datetime.datetime.now().strftime("%Y-%m-%d")

            # スケジュールの開始/終了時間
            start_h, start_m, end_h, end_m = parse_time_range(time_str)
            # entry_dt の日付に合わせる( entry_dtがNoneなら現在時刻を仮使用など要調整 )
            base_dt = entry_dt if entry_dt else datetime.datetime.now()
            start_dt = combine_date_time(base_dt, start_h, start_m)
            finish_dt = combine_date_time(base_dt, end_h, end_m)

            # 出欠判定
            attend_status, fix_entry_next, fix_exit_cur, fix_exit_next, note = judge_attendance(
                entry_dt, exit_dt, start_dt, finish_dt
            )
            mark = attend_status
            if note:
                mark = f"{attend_status}({note})"

            # まとめ
            attendance_result_dict[(student_index, date_str, int_course_id)] = mark

            # Firebase更新 (exit修正/次entry修正/次exit修正)
            if fix_exit_cur is not None:
                exit_key_path = f"Students/attendance/student_id/{student_id}/exit{idx_str}"
                fix_data = {
                    "read_datetime": fix_exit_cur.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": schedule_info.get("serial_number", "")
                }
                firebase_updates.append((exit_key_path, fix_data))

            if fix_entry_next is not None:
                next_idx = str(int(idx_str) + 1)
                entry_key_path = f"Students/attendance/student_id/{student_id}/entry{next_idx}"
                fix_data = {
                    "read_datetime": fix_entry_next.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": schedule_info.get("serial_number", "")
                }
                firebase_updates.append((entry_key_path, fix_data))

            if fix_exit_next is not None:
                next_idx = str(int(idx_str) + 1)
                exit_key_path = f"Students/attendance/student_id/{student_id}/exit{next_idx}"
                fix_data = {
                    "read_datetime": fix_exit_next.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": schedule_info.get("serial_number", "")
                }
                firebase_updates.append((exit_key_path, fix_data))

            pair_idx += 1

    # Firebase 更新
    for path, val in firebase_updates:
        update_data_in_firebase(path, val)

    # シート書き込み
    student_info_index_data = get_data_from_firebase("Students/student_info/student_index")
    if not student_info_index_data:
        print("No student_info index data.")
        return

    sheets_cache = {}
    for (st_idx, date_str, c_id), mark in attendance_result_dict.items():
        st_info = student_info_index_data.get(st_idx)
        if not st_info or "sheet_id" not in st_info:
            continue
        sheet_id = st_info["sheet_id"]

        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        sheet_name = date_obj.strftime("%Y-%m")

        if (sheet_id, sheet_name) not in sheets_cache:
            try:
                sh = gspread_client.open_by_key(sheet_id)
                try:
                    ws = sh.worksheet(sheet_name)
                except gspread.exceptions.WorksheetNotFound:
                    ws = sh.add_worksheet(title=sheet_name, rows="100", cols="100")
                sheets_cache[(sheet_id, sheet_name)] = ws
            except Exception as e:
                print(f"Error opening sheet {sheet_id}: {e}")
                continue

        worksheet = sheets_cache[(sheet_id, sheet_name)]
        col_idx = date_obj.day + 1
        row_idx = c_id + 1
        worksheet.update_cell(row_idx, col_idx, mark)

    print("Done.")

# ===============================
# 実行ブロック
# ===============================
if __name__ == "__main__":
    main_process()
