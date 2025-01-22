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
    """Firebase Realtime Database の path からデータを取得して返す"""
    ref = db.reference(path)
    return ref.get()

def update_data_in_firebase(path, data):
    """Firebase Realtime Database の path にデータを保存(更新)する"""
    ref = db.reference(path)
    ref.update(data)

# ===============================
# 時刻・日付パース用ヘルパー関数
# ===============================
def parse_datetime(dt_str):
    """
    例: "2025-01-06 08:49:50" -> datetime.datetime(2025, 1, 6, 8, 49, 50)
    """
    return datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")

def parse_time_range(time_str):
    """
    例: "8:50~10:20" -> (8, 50, 10, 20)
    """
    start_str, end_str = time_str.split("~")
    start_h, start_m = map(int, start_str.split(":"))
    end_h, end_m = map(int, end_str.split(":"))
    return start_h, start_m, end_h, end_m

def combine_date_time(base_dt, hour, minute):
    """
    base_dt: datetime (日付部分を利用)
    時刻(hour, minute)を合わせた新しいdatetimeを返す
    """
    return datetime.datetime(base_dt.year, base_dt.month, base_dt.day, hour, minute)

# ===============================
# 出欠判定ロジック(条件②,③修正済み)
# ===============================
def judge_attendance(entry_dt, exit_dt, start_dt, finish_dt):
    """
    entry_dt: 入室時刻(datetime)
    exit_dt : 退室時刻(datetime) / 存在しない(None)場合あり
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
    delta_5min = datetime.timedelta(minutes=5)
    delta_10min = datetime.timedelta(minutes=10)

    # entry_dtが無い場合は欠席とみなす
    if entry_dt is None:
        return ("✕", None, None, None, "")

    # -----------------------
    # (②) exit が finish+5分「以降」であれば、
    #  一旦 exit1を別変数に保管、exit1=finish1、entry2=finish1+10分、exit2=old_exit
    #  コース1は「〇(正常出席)」
    # -----------------------
    if exit_dt is not None and exit_dt >= finish_dt + delta_5min and entry_dt <= start_dt + delta_5min:
        old_exit = exit_dt
        fix_exit_cur = finish_dt              # exit1を授業終了時刻に合わせる
        fix_entry_next = finish_dt + delta_10min  # 次コースのentry
        fix_exit_next = old_exit             # 次コースのexitは もとのexitを使う
        return ("〇", fix_entry_next, fix_exit_cur, fix_exit_next, "")

    # -----------------------
    # (③) exit が存在しない (None) => exit1=finish1, entry2=finish1+10分
    #  コース1は「〇(正常出席)」
    # -----------------------
    if exit_dt is None and entry_dt <= start_dt + delta_5min:
        fix_exit_cur = finish_dt
        fix_entry_next = finish_dt + delta_10min
        fix_exit_next = None  # 次コースのexitはとりあえず作らない
        return ("〇", fix_entry_next, fix_exit_cur, fix_exit_next, "")

    # -----------------------
    # 以下、その他の判定(早退/遅刻/欠席/正常出席)
    # -----------------------

    # 早退 (△早) : entryが start+5分以内 かつ exit <= finish-5分
    if (entry_dt <= start_dt + delta_5min) and \
       (exit_dt is not None) and (exit_dt <= finish_dt - delta_5min):
        diff_min = int((finish_dt - exit_dt).total_seconds() // 60)
        note_str = f"△早{diff_min}分"
        return ("△早", None, None, None, note_str)

    # 遅刻 (△遅) : entryが start+5分以降 かつ exit <= finish+5分
    if (entry_dt > start_dt + delta_5min) and \
       (exit_dt is not None) and (exit_dt <= finish_dt + delta_5min):
        diff_min = int((entry_dt - start_dt).total_seconds() // 60)
        note_str = f"△遅{diff_min}分"
        return ("△遅", None, None, None, note_str)

    # 正常出席(〇) : entry <= start+5分 かつ exit <= finish+5分
    if (entry_dt <= start_dt + delta_5min) and \
       (exit_dt is not None) and (exit_dt <= finish_dt + delta_5min):
        return ("〇", None, None, None, "")

    # どの条件にも当てはまらない場合 => 欠席(✕)
    return ("✕", None, None, None, "")

# ===============================
# メイン処理
# ===============================
def main_process():
    # ====================================================
    # 1) Students/attendance/student_id 以下をループ
    # ====================================================
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("No attendance data.")
        return

    # コース全体情報を一括取得 (配列形式: index=course_id)
    courses_data = get_data_from_firebase("Courses/course_id")

    # 出席結果を一時保存する辞書:
    #  {(student_index, date_str, course_id) : "〇"/"✕"/"△早"/"△遅"など}
    attendance_result_dict = {}

    # Firebaseへまとめて更新するためのリスト [(path, data), ...]
    firebase_updates = []

    # -------------------
    # attendance配下の student_id ごとに処理
    # -------------------
    for student_id, entries_exits_dict in attendance_data.items():
        if not entries_exits_dict:
            continue

        # 2) student_info > student_id/{student_id} から student_index を取得
        path_info = f"Students/student_info/student_id/{student_id}"
        student_info = get_data_from_firebase(path_info)
        if not student_info or "student_index" not in student_info:
            continue
        student_index = student_info["student_index"]

        # 3) enrollment > student_index/{student_index}/course_id を取得
        path_enroll = f"Students/enrollment/student_index/{student_index}"
        enroll_info = get_data_from_firebase(path_enroll)
        if not enroll_info or "course_id" not in enroll_info:
            continue
        
        course_id_str = enroll_info["course_id"]  # 例 "1, 2"
        course_ids = [cid.strip() for cid in course_id_str.split(",") if cid.strip()]

        # entry, exit のペアを作る
        # (例: entry1, exit1, entry2, exit2, ...)
        sorted_keys = sorted(entries_exits_dict.keys())
        entry_exit_pairs = []
        current_entry = None
        current_exit = None
        current_idx = None

        for k in sorted_keys:
            v = entries_exits_dict[k]
            if not isinstance(v, dict):
                continue

            dt_str = v.get("read_datetime")
            if not dt_str:
                continue

            # key: "entry1" or "exit1" など
            if k.startswith("entry"):
                current_idx = k.replace("entry", "")
                current_entry = parse_datetime(dt_str)
            elif k.startswith("exit"):
                current_idx = k.replace("exit", "")
                current_exit = parse_datetime(dt_str)

            # entryとexitが揃ったらペアにまとめる
            if current_entry and current_exit:
                entry_exit_pairs.append((current_idx, current_entry, current_exit))
                current_idx = None
                current_entry = None
                current_exit = None

        # entryだけあってexitが無いケースを最後に追加
        if current_entry and not current_exit:
            entry_exit_pairs.append((current_idx, current_entry, None))

        # -------------------------
        # (loop3) course_idsに基づき、1つずつコースを処理
        # -------------------------
        pair_idx = 0  # entry_exit_pairs のインデックス
        for i, course_id in enumerate(course_ids, start=1):
            # course_idが数字でcourses_dataに存在するか確認
            try:
                int_course_id = int(course_id)
            except ValueError:
                # 数字でなければスキップ
                continue

            if not courses_data or int_course_id >= len(courses_data) or courses_data[int_course_id] is None:
                continue

            course_info = courses_data[int_course_id]
            schedule_info = course_info.get("schedule", {})
            time_str = schedule_info.get("time")  # "8:50~10:20" など

            if not time_str:
                # スケジュール無いコースは飛ばす
                continue

            # entry_exit_pairsが足りなければ => 欠席(✕)
            if pair_idx >= len(entry_exit_pairs):
                date_str = datetime.datetime.now().strftime("%Y-%m-%d")  # とりあえず本日をキーとする例
                attendance_result_dict[(student_index, date_str, int_course_id)] = "✕"
                continue

            idx_str, entry_dt, exit_dt = entry_exit_pairs[pair_idx]
            # 日付文字列
            date_str = entry_dt.strftime("%Y-%m-%d")

            # コース開始終了時刻
            start_h, start_m, end_h, end_m = parse_time_range(time_str)
            start_dt = combine_date_time(entry_dt, start_h, start_m)
            finish_dt = combine_date_time(entry_dt, end_h, end_m)

            # 出欠判定 (修正後)
            attend_status, fix_entry_next, fix_exit_cur, fix_exit_next, note = judge_attendance(
                entry_dt, exit_dt, start_dt, finish_dt
            )

            # 備考(note)があればマークに付加
            result_mark = attend_status
            if note:
                result_mark = f"{attend_status}({note})"

            # 結果を一時保存
            attendance_result_dict[(student_index, date_str, int_course_id)] = result_mark

            # --------------------------------------------------
            # judge_attendance の結果に応じて Firebase 更新
            # --------------------------------------------------
            # (A) このコースの exit を修正する場合
            if fix_exit_cur is not None:
                exit_key_path = f"Students/attendance/student_id/{student_id}/exit{idx_str}"
                fix_data = {
                    "read_datetime": fix_exit_cur.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": schedule_info.get("serial_number", "")
                }
                firebase_updates.append((exit_key_path, fix_data))

            # (B) 次コースの entry を修正する場合
            if fix_entry_next is not None:
                next_idx = str(int(idx_str) + 1)
                entry_key_path = f"Students/attendance/student_id/{student_id}/entry{next_idx}"
                fix_data = {
                    "read_datetime": fix_entry_next.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": schedule_info.get("serial_number", "")
                }
                firebase_updates.append((entry_key_path, fix_data))

            # (C) 次コースの exit を作成/修正する場合
            if fix_exit_next is not None:
                next_idx = str(int(idx_str) + 1)
                exit_key_path = f"Students/attendance/student_id/{student_id}/exit{next_idx}"
                fix_data = {
                    "read_datetime": fix_exit_next.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": schedule_info.get("serial_number", "")
                }
                firebase_updates.append((exit_key_path, fix_data))

            # 次のコースへ
            pair_idx += 1

    # -------------------------
    # 上記ループ終了後、firebase_updates を一括更新
    # -------------------------
    for path, val in firebase_updates:
        update_data_in_firebase(path, val)

    # -------------------------
    # 最後に、シートへの書き込み
    #   student_info > student_index から sheet_id を取得し、
    #   「%Y-%m」シートの (row=course_id+1, col=day+1) に記録
    # -------------------------
    student_info_index_data = get_data_from_firebase("Students/student_info/student_index")
    if not student_info_index_data:
        print("No student_info index data.")
        return

    # シート取得キャッシュ {(sheet_id, 'YYYY-MM'): worksheet}
    sheets_cache = {}

    for (st_idx, date_str, c_id), mark in attendance_result_dict.items():
        st_info = student_info_index_data.get(st_idx)
        if not st_info or "sheet_id" not in st_info:
            continue
        target_sheet_id = st_info["sheet_id"]

        # シート名 = "YYYY-MM"
        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        sheet_name = date_obj.strftime("%Y-%m")

        if (target_sheet_id, sheet_name) not in sheets_cache:
            # 該当スプレッドシート & ワークシートを取得
            try:
                sh = gspread_client.open_by_key(target_sheet_id)
                try:
                    ws = sh.worksheet(sheet_name)
                except gspread.exceptions.WorksheetNotFound:
                    # 存在しない場合は新規作成する例
                    ws = sh.add_worksheet(title=sheet_name, rows="100", cols="100")
                sheets_cache[(target_sheet_id, sheet_name)] = ws
            except Exception as e:
                print(f"Error opening sheet {target_sheet_id}: {e}")
                continue

        worksheet = sheets_cache[(target_sheet_id, sheet_name)]

        # 列 = 日付(%d) + 1
        col_idx = date_obj.day + 1
        # 行 = course_id + 1 (course_idが1始まりなのでこのように)
        row_idx = c_id + 1

        # セルへ書き込み
        worksheet.update_cell(row_idx, col_idx, mark)

    print("Done.")

# ===============================
# 実行エントリポイント
# ===============================
if __name__ == "__main__":
    main_process()
