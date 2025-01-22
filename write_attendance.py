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
    (datetime.time(8,50), datetime.time(10,20)) に変換して返す
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
    date_dt(年月日) + time_obj(時分秒) で新しい datetimeを返す
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
# 出欠判定 (①②③ を優先的に)
# ---------------------
def judge_attendance_for_course(entry_dt, exit_dt, start_dt, finish_dt):
    """
    ① entry <= start+5分 and exit <= finish+5分 → 正常(○)
    ② exit > finish+5分 → 
       - original_exit = exit
       - exit = finish
       - entry2 = finish+10分
       - exit2 = original_exit
       - 結果ステータス = 正常(○)
    ③ exit が None → exit=finish, entry2=finish+10分 → 正常(○)

    # 参考: 下記のようなロジックを挿入する場合はご自由に追記:
    # 欠席(×):   if entry_dt >= finish_dt: return "×", ...
    # 早退(△早): ...
    # 遅刻(△遅): ...
    """
    td_5min = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    # ---- (例) 欠席判定をするならここで書く ----
    # if entry_dt >= finish_dt:
    #     return "×", entry_dt, exit_dt, None

    # ---- ① ----
    if (entry_dt <= (start_dt + td_5min)):
        # exitがあれば比較する
        if exit_dt is not None:
            if exit_dt <= (finish_dt + td_5min):
                # ①の条件に合致 → 正常
                return "○", entry_dt, exit_dt, None
        else:
            # exit_dt が None なら ③のほうへ移る(ロジック上は同じように扱うので後段へ)
            pass

    # ---- ② exitが finish+5分 以降 ----
    if exit_dt is not None and exit_dt > (finish_dt + td_5min):
        # exitをfinishに補正
        original_exit = exit_dt
        updated_exit_dt = finish_dt
        # 次コマのentryを finish+10分 に
        next_course_entry = finish_dt + td_10min
        # exit2は original_exit
        # → 戻り値として next_course_entry_dt だけ返し、呼び出し側で exit2を生成する

        return "○", entry_dt, updated_exit_dt, (next_course_entry, original_exit)

    # ---- ③ exitが None → exitをfinishに
    if exit_dt is None:
        # 正常(○)
        updated_exit_dt = finish_dt
        # 次コマエントリ = finish+10分
        next_course_entry = finish_dt + td_10min
        # exit2 は無い(= None) ため Noneを一緒に返す
        return "○", entry_dt, updated_exit_dt, (next_course_entry, None)

    # ---- ①の後半 ----
    # 「entry <= start+5分」の判定を抜けた or exit_dtあり/なしが合わないなどの場合、
    # 必要に応じて別のステータスを返す(早退/遅刻 等)。
    # ここでは一旦すべて「？」に
    return "？", entry_dt, exit_dt, None

# ---------------------
# メインフロー
# ---------------------
def process_attendance_and_write_sheet():
    # Students/attendance
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("attendanceデータがありません。終了します。")
        return

    # Courses (0番目: None想定)
    courses_data = get_data_from_firebase("Courses/course_id")
    # student_info
    student_info_data = get_data_from_firebase("Students/student_info")

    results_dict = {}  # {(student_index, idx_in_list, yyyymmdd) : status}

    # -----------------
    # 受講生ごとにループ
    # -----------------
    for student_id, attendance_dict in attendance_data.items():
        if not isinstance(attendance_dict, dict):
            continue

        # student_index 取得
        student_index = None
        if (student_info_data.get("student_id")
            and student_id in student_info_data["student_id"]
            and "student_index" in student_info_data["student_id"][student_id]):
            student_index = student_info_data["student_id"][student_id]["student_index"]
        if not student_index:
            continue

        # enrollment から コースID取得
        enroll_path = f"Students/enrollment/student_index/{student_index}"
        enrollment_data = get_data_from_firebase(enroll_path)
        if not enrollment_data or "course_id" not in enrollment_data:
            continue
        course_id_str = enrollment_data["course_id"]  # "1, 2" など
        course_id_list = [x.strip() for x in course_id_str.split(",") if x.strip()]
        if not course_id_list:
            continue

        # attendance_dict 内の entry/exit ペアを抽出
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
            # entryが一つも無い → 全コース欠席 or スキップ
            # ここではスキップ例
            continue

        # base_date: 最初のentry_dtの日付を「同日の授業」として扱う
        first_entry_key, _ = entry_exit_pairs[0]
        first_entry_str = attendance_dict[first_entry_key].get("read_datetime", "")
        first_entry_dt = parse_datetime(first_entry_str)
        if not first_entry_dt:
            continue

        base_date = first_entry_dt.date()  # datetime.date

        pair_index = 0

        # -----------------
        # コース順に処理
        # -----------------
        for idx, c_id_str in enumerate(course_id_list, start=1):
            # idx は 1,2,3,...(この学生が持つコースの並び順)
            # c_id_str 例: "1","2"
            try:
                c_id_int = int(c_id_str)
            except:
                continue
            if c_id_int <= 0 or c_id_int >= len(courses_data):
                continue

            course_info = courses_data[c_id_int]
            schedule_info = course_info.get("schedule", {})
            time_range_str = schedule_info.get("time", "")
            if not time_range_str:
                continue

            if pair_index >= len(entry_exit_pairs):
                # ペアがもう無い → 欠席(×)とする or 何もしない
                # ここでは ×
                date_str = base_date.strftime("%Y-%m-%d")
                results_dict[(student_index, idx, date_str)] = "×"
                continue

            ekey, xkey = entry_exit_pairs[pair_index]
            pair_index += 1

            entry_rec = attendance_dict.get(ekey, {})
            exit_rec  = attendance_dict.get(xkey, {})
            entry_dt_str = entry_rec.get("read_datetime")
            exit_dt_str  = exit_rec.get("read_datetime")

            entry_dt = parse_datetime(entry_dt_str) if entry_dt_str else None
            exit_dt  = parse_datetime(exit_dt_str)  if exit_dt_str else None

            if not entry_dt:
                # entryが無ければ欠席
                date_str = base_date.strftime("%Y-%m-%d")
                results_dict[(student_index, idx, date_str)] = "×"
                continue

            # コース開始/終了は base_date + time
            start_t, finish_t = parse_hhmm_range(time_range_str)
            if not start_t or not finish_t:
                continue
            start_dt = combine_date_and_time(base_date, start_t)
            finish_dt = combine_date_and_time(base_date, finish_t)

            # 出欠判定 (①②③)
            status, new_entry_dt, new_exit_dt, next_course_data = judge_attendance_for_course(
                entry_dt, exit_dt, start_dt, finish_dt
            )
            # next_course_data はタプル(next_course_entry, next_course_exit) か None

            # Firebase更新
            updates = {}
            if new_entry_dt != entry_dt:
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_rec.get("serial_number", "")
                }
            if new_exit_dt != exit_dt:
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_rec.get("serial_number", "")
                }

            # ②or③ で exit2 を作る場合
            # next_course_data = ( next_entry2, next_exit2 ) or None
            if next_course_data:
                next_ekey = f"entry{pair_index+1}"
                next_xkey = f"exit{pair_index+1}"
                next_e, next_x = next_course_data  # それぞれ datetime or None
                if next_e is not None:
                    updates[next_ekey] = {
                        "read_datetime": next_e.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": entry_rec.get("serial_number", "")
                    }
                if next_x is not None:
                    updates[next_xkey] = {
                        "read_datetime": next_x.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": exit_rec.get("serial_number", "")
                    }

            if updates:
                att_path = f"Students/attendance/student_id/{student_id}"
                update_data_in_firebase(att_path, updates)

            # シート書き込み用に結果を格納
            date_str = base_date.strftime("%Y-%m-%d")
            results_dict[(student_index, idx, date_str)] = status

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
            print(f"シート({sheet_id}) オープン失敗: {e}")
            continue

        # 当該 student_index の結果のみ取り出し
        std_result_items = {
            k: v for k, v in results_dict.items() if k[0] == std_idx
        }
        if not std_result_items:
            continue

        # 書き込み
        for (s_idx, course_idx, date_str), status_val in std_result_items.items():
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # %d (01～31)

            # Worksheet取得 or 新規作成
            try:
                ws = sh.worksheet(yyyymm)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            # 行 = 日付 +1
            row = day + 1
            # 列 = その学生のコース順 (idx) +1
            col = course_idx + 1

            try:
                ws.update_cell(row, col, status_val)
            except Exception as e:
                print(f"シート書き込み失敗 [{sheet_id}:{yyyymm} R{row}C{col}]: {e}")

    print("=== 出席判定とシート書き込みが完了しました ===")

# ---------------------
# 実行
# ---------------------
if __name__ == "__main__":
    process_attendance_and_write_sheet()
