import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ===============================
# Firebase・GSpread初期化
# ===============================
print("=== Initializing Firebase and Google Sheets... ===")
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    print("Firebase credentials loaded.")
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })
    print("Firebase initialized.")

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gspread_client = gspread.authorize(creds)
print("Google Sheets client authorized.")

# ===============================
# Firebaseヘルパー関数
# ===============================
def get_data_from_firebase(path):
    """Firebase Realtime Database の path からデータを取得して返す"""
    print(f"Fetching data from Firebase path: {path}")
    ref = db.reference(path)
    data = ref.get()
    print(f"Data fetched from {path}: {data}")
    return data

def update_data_in_firebase(path, data):
    """Firebase Realtime Database の path にデータを保存(更新)する"""
    print(f"Updating data in Firebase path: {path} with data: {data}")
    ref = db.reference(path)
    ref.update(data)
    print("Update completed.")

# ===============================
# 時刻・日付パース用ヘルパー
# ===============================
def parse_datetime(dt_str):
    """
    例: "2025-01-06 08:49:50" -> datetime.datetime(2025, 1, 6, 8, 49, 50)
    """
    print(f"Parsing datetime string: {dt_str}")
    dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    print(f"Parsed datetime: {dt}")
    return dt

def parse_time_range(time_str):
    """
    例: "8:50~10:20" -> (8, 50, 10, 20)
    """
    print(f"Parsing time range string: {time_str}")
    start_str, end_str = time_str.split("~")
    start_h, start_m = map(int, start_str.split(":"))
    end_h, end_m = map(int, end_str.split(":"))
    print(f"Parsed time range -> start: {start_h}:{start_m}, end: {end_h}:{end_m}")
    return start_h, start_m, end_h, end_m

def combine_date_time(base_dt, hour, minute):
    """
    base_dt: datetime
    時刻(hour, minute)を合わせた新しいdatetimeを返す
    """
    print(f"Combining date from {base_dt} with time {hour}:{minute}...")
    combined = datetime.datetime(base_dt.year, base_dt.month, base_dt.day, hour, minute)
    print(f"Combined datetime: {combined}")
    return combined

# ===============================
# 出欠判定ロジック
# ===============================
def judge_attendance(entry_dt, exit_dt, start_dt, finish_dt):
    """
    entry_dt: 入室時刻(datetime)
    exit_dt: 退室時刻(datetime) / 存在しない(None)場合あり
    start_dt: コース開始時刻(datetime)
    finish_dt:コース終了時刻(datetime)

    戻り値:
      (attend_status, fix_entry_next, fix_exit_cur, fix_exit_next, note)
        - attend_status : "〇", "✕", "△早", "△遅" などの出欠ステータス
        - fix_entry_next: 次コースのentryを強制的に書き換える場合の時刻 (Noneなら修正不要)
        - fix_exit_cur  : このコースのexitを修正する場合の時刻 (Noneなら修正不要)
        - fix_exit_next : 次コースのexitを強制的に作成する場合の時刻 (Noneなら不要)
        - note          : 付加情報("△早10分"など)
    """
    print("\n--- judge_attendance called ---")
    print(f"entry_dt = {entry_dt}, exit_dt = {exit_dt}")
    print(f"start_dt = {start_dt}, finish_dt = {finish_dt}")

    delta_5min = datetime.timedelta(minutes=5)
    delta_10min = datetime.timedelta(minutes=10)

    # entry_dtが無い場合は欠席
    if entry_dt is None:
        print("No entry_dt -> treating as absence (✕).")
        return ("✕", None, None, None, "")

    # --- 条件② ---
    # exit が finish+5分 以降
    if exit_dt is not None and exit_dt >= finish_dt + delta_5min and entry_dt <= start_dt + delta_5min:
        print("Condition② triggered: exit >= finish+5min, entry <= start+5min -> Normal attendance, fix times.")
        old_exit = exit_dt
        fix_exit_cur = finish_dt                # 現コースのexit
        fix_entry_next = finish_dt + delta_10min  # 次コースのentry
        fix_exit_next = old_exit               # 次コースのexit
        print(f"fix_exit_cur={fix_exit_cur}, fix_entry_next={fix_entry_next}, fix_exit_next={fix_exit_next}")
        return ("〇", fix_entry_next, fix_exit_cur, fix_exit_next, "")

    # --- 条件③ ---
    # exit が None (押し忘れ)
    if exit_dt is None and entry_dt <= start_dt + delta_5min:
        print("Condition③ triggered: exit_dt is None, entry <= start+5min -> Normal attendance, fix times.")
        fix_exit_cur = finish_dt
        fix_entry_next = finish_dt + delta_10min
        fix_exit_next = None
        print(f"fix_exit_cur={fix_exit_cur}, fix_entry_next={fix_entry_next}, fix_exit_next={fix_exit_next}")
        return ("〇", fix_entry_next, fix_exit_cur, fix_exit_next, "")

    # 早退 (△早)
    if (entry_dt <= start_dt + delta_5min) and \
       (exit_dt is not None) and (exit_dt <= finish_dt - delta_5min):
        diff_min = int((finish_dt - exit_dt).total_seconds() // 60)
        note_str = f"△早{diff_min}分"
        print(f"Early leave (△早) detected. note_str={note_str}")
        return ("△早", None, None, None, note_str)

    # 遅刻 (△遅)
    if (entry_dt > start_dt + delta_5min) and \
       (exit_dt is not None) and (exit_dt <= finish_dt + delta_5min):
        diff_min = int((entry_dt - start_dt).total_seconds() // 60)
        note_str = f"△遅{diff_min}分"
        print(f"Late arrival (△遅) detected. note_str={note_str}")
        return ("△遅", None, None, None, note_str)

    # 正常出席 (〇)
    if (entry_dt <= start_dt + delta_5min) and \
       (exit_dt is not None) and (exit_dt <= finish_dt + delta_5min):
        print("Normal attendance (〇).")
        return ("〇", None, None, None, "")

    # どの条件にも当てはまらない => 欠席
    print("No matching condition -> Absence (✕).")
    return ("✕", None, None, None, "")

# ===============================
# メイン処理
# ===============================
def main_process():
    print("=== main_process start ===")

    # 1) Attendance データ取得
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("No attendance data found. Exiting.")
        return

    # コース全体情報
    courses_data = get_data_from_firebase("Courses/course_id")

    # 出席結果を一時保存
    attendance_result_dict = {}
    # Firebase更新リスト
    firebase_updates = []

    # attendance配下の student_id ごとにループ
    for student_id, entries_exits_dict in attendance_data.items():
        print(f"\n--- Processing student_id: {student_id} ---")

        if not entries_exits_dict:
            print(f"No entries/exits found for student_id={student_id}, skipping.")
            continue

        # 2) student_info から student_index を取得
        path_info = f"Students/student_info/student_id/{student_id}"
        student_info = get_data_from_firebase(path_info)
        if not student_info or "student_index" not in student_info:
            print(f"student_index not found for student_id={student_id}, skipping.")
            continue
        student_index = student_info["student_index"]
        print(f"Found student_index: {student_index}")

        # 3) enrollment からコースID一覧
        path_enroll = f"Students/enrollment/student_index/{student_index}"
        enroll_info = get_data_from_firebase(path_enroll)
        if not enroll_info or "course_id" not in enroll_info:
            print(f"No course_id enrollment info for student_index={student_index}, skipping.")
            continue
        
        course_id_str = enroll_info["course_id"]
        course_ids = [cid.strip() for cid in course_id_str.split(",") if cid.strip()]
        print(f"Course IDs for {student_index}: {course_ids}")

        # entry/exit のペアを作る
        sorted_keys = sorted(entries_exits_dict.keys())
        entry_exit_pairs = []
        current_entry = None
        current_exit = None
        current_idx = None

        for k in sorted_keys:
            v = entries_exits_dict[k]
            print(f"Key={k}, Value={v}")
            if not isinstance(v, dict):
                print(f"Skipping key={k} because value is not dict.")
                continue

            dt_str = v.get("read_datetime")
            if not dt_str:
                print(f"No read_datetime in {k}, skipping.")
                continue

            if k.startswith("entry"):
                current_idx = k.replace("entry", "")
                current_entry = parse_datetime(dt_str)
            elif k.startswith("exit"):
                current_idx = k.replace("exit", "")
                current_exit = parse_datetime(dt_str)

            # ペアが揃ったら追加
            if current_entry and current_exit:
                entry_exit_pairs.append((current_idx, current_entry, current_exit))
                print(f"Appended pair: (index={current_idx}, entry={current_entry}, exit={current_exit})")
                current_idx = None
                current_entry = None
                current_exit = None

        # entryだけ残っていたら追加
        if current_entry and not current_exit:
            entry_exit_pairs.append((current_idx, current_entry, None))
            print(f"Appended incomplete pair (missing exit): (index={current_idx}, entry={current_entry}, exit=None)")

        # コースごとに出欠判定
        pair_idx = 0
        for i, course_id in enumerate(course_ids, start=1):
            print(f"\nProcessing course_id={course_id} for student_index={student_index}...")

            try:
                int_course_id = int(course_id)
            except ValueError:
                print(f"course_id={course_id} is not a valid int, skipping.")
                continue

            if not courses_data or int_course_id >= len(courses_data) or courses_data[int_course_id] is None:
                print(f"No valid course data found for course_id={int_course_id}, skipping.")
                continue

            course_info = courses_data[int_course_id]
            schedule_info = course_info.get("schedule", {})
            time_str = schedule_info.get("time")
            if not time_str:
                print(f"No schedule time for course_id={int_course_id}, skipping.")
                continue

            if pair_idx >= len(entry_exit_pairs):
                # ペア不足 => 欠席扱い
                date_str = datetime.datetime.now().strftime("%Y-%m-%d")
                attendance_result_dict[(student_index, date_str, int_course_id)] = "✕"
                print(f"No more entry_exit_pairs. Marking absent (✕) for date={date_str}.")
                continue

            idx_str, entry_dt, exit_dt = entry_exit_pairs[pair_idx]
            date_str = entry_dt.strftime("%Y-%m-%d")
            print(f"Using pair_idx={pair_idx}, idx_str={idx_str}, entry_dt={entry_dt}, exit_dt={exit_dt}, date_str={date_str}")

            # スケジュール時刻
            start_h, start_m, end_h, end_m = parse_time_range(time_str)
            start_dt = combine_date_time(entry_dt, start_h, start_m)
            finish_dt = combine_date_time(entry_dt, end_h, end_m)

            # 出欠判定
            (attend_status,
             fix_entry_next,
             fix_exit_cur,
             fix_exit_next,
             note) = judge_attendance(entry_dt, exit_dt, start_dt, finish_dt)

            # マークにnoteを付加
            result_mark = attend_status
            if note:
                result_mark = f"{attend_status}({note})"

            # 結果保存
            attendance_result_dict[(student_index, date_str, int_course_id)] = result_mark
            print(f"Attendance result -> {result_mark} for (student_index={student_index}, date_str={date_str}, course_id={int_course_id})")

            # Firebase 更新
            # (A) 現コース exit 強制修正
            if fix_exit_cur is not None:
                exit_key_path = f"Students/attendance/student_id/{student_id}/exit{idx_str}"
                fix_data = {
                    "read_datetime": fix_exit_cur.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": schedule_info.get("serial_number", "")
                }
                print(f"Queueing fix_exit_cur update -> path={exit_key_path}, data={fix_data}")
                firebase_updates.append((exit_key_path, fix_data))

            # (B) 次コース entry 修正
            if fix_entry_next is not None:
                next_idx = str(int(idx_str) + 1)
                entry_key_path = f"Students/attendance/student_id/{student_id}/entry{next_idx}"
                fix_data = {
                    "read_datetime": fix_entry_next.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": schedule_info.get("serial_number", "")
                }
                print(f"Queueing fix_entry_next update -> path={entry_key_path}, data={fix_data}")
                firebase_updates.append((entry_key_path, fix_data))

            # (C) 次コース exit 修正
            if fix_exit_next is not None:
                next_idx = str(int(idx_str) + 1)
                exit_key_path = f"Students/attendance/student_id/{student_id}/exit{next_idx}"
                fix_data = {
                    "read_datetime": fix_exit_next.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": schedule_info.get("serial_number", "")
                }
                print(f"Queueing fix_exit_next update -> path={exit_key_path}, data={fix_data}")
                firebase_updates.append((exit_key_path, fix_data))

            # 次のコースへ
            pair_idx += 1
        # end for (course_ids)

    # end for (attendance_data)

    print("\n=== Updating Firebase with queued changes... ===")
    for path, val in firebase_updates:
        update_data_in_firebase(path, val)

    # シート書き込み
    print("\n=== Writing results to Google Sheets... ===")
    student_info_index_data = get_data_from_firebase("Students/student_info/student_index")
    if not student_info_index_data:
        print("No student_info index data, cannot write to sheets.")
        return

    sheets_cache = {}
    for (st_idx, date_str, c_id), mark in attendance_result_dict.items():
        print(f"\nProcessing final result for st_idx={st_idx}, date_str={date_str}, course_id={c_id}, mark={mark}")
        st_info = student_info_index_data.get(st_idx)
        if not st_info or "sheet_id" not in st_info:
            print(f"No sheet_id found in student_info for {st_idx}, skipping.")
            continue
        target_sheet_id = st_info["sheet_id"]

        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        sheet_name = date_obj.strftime("%Y-%m")
        print(f"Target sheet_id={target_sheet_id}, sheet_name={sheet_name}")

        if (target_sheet_id, sheet_name) not in sheets_cache:
            print(f"Opening sheet: {target_sheet_id}, looking for worksheet: {sheet_name}")
            try:
                sh = gspread_client.open_by_key(target_sheet_id)
                try:
                    ws = sh.worksheet(sheet_name)
                except gspread.exceptions.WorksheetNotFound:
                    print(f"Worksheet '{sheet_name}' not found; creating new worksheet.")
                    ws = sh.add_worksheet(title=sheet_name, rows="100", cols="100")
                sheets_cache[(target_sheet_id, sheet_name)] = ws
                print("Worksheet cached.")
            except Exception as e:
                print(f"Error opening sheet {target_sheet_id}: {e}")
                continue

        worksheet = sheets_cache[(target_sheet_id, sheet_name)]

        col_idx = date_obj.day + 1
        row_idx = c_id + 1
        print(f"Updating cell(row={row_idx}, col={col_idx}) with '{mark}'")
        worksheet.update_cell(row_idx, col_idx, mark)

    print("=== main_process complete ===")

# ===============================
# 実行エントリポイント
# ===============================
if __name__ == "__main__":
    main_process()
