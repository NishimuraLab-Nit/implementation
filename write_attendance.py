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
    """文字列 → datetime に変換。失敗時は None を返す"""
    try:
        return datetime.datetime.strptime(dt_str, fmt)
    except Exception:
        return None

def parse_hhmm_range(range_str):
    """
    "8:50~10:20" のような文字列を (time(8,50), time(10,20)) に変換して返す
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
    date_dt(年月日) + time_obj(時分秒) → 新たな datetime を返す
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
# 出欠判定ロジック(①②③)
# ---------------------
def judge_attendance_for_course(entry_dt, exit_dt, start_dt, finish_dt):
    """
    出席判定ロジックまとめ:
      # 欠席 (×):
        - entry_dt >= finish_dt
      # 正常出席 (○):
        ① entry_dt <= start_dt+5min & exit_dt <= finish_dt+5min
        ② exit_dt > finish_dt+5min
        ③ exit_dt is None
      # 早退 (△早):
        entry_dt <= start_dt+5min & exit_dt < finish_dt-5min
        "△早xx分" (xxは finish_dt - exit_dt)
      # 遅刻 (△遅):
        entry_dt > start_dt+5min & exit_dt <= finish_dt+5min
        "△遅xx分" (xxは entry_dt - start_dt)

    戻り値:
      (status_str, updated_entry_dt, updated_exit_dt, next_course_data)
        - status_str: "×", "○", "△早xx分", "△遅xx分", "？" 等
        - updated_entry_dt, updated_exit_dt: 補正した entry/exit
        - next_course_data: (next_entry_dt, next_exit_dt) or None
          (②,③ のときに生成)
    """
    td_5min  = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    # (1) 欠席(×)
    if entry_dt >= finish_dt:
        return "×", entry_dt, exit_dt, None

    # (2) 正常出席(○) ①
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt <= (finish_dt + td_5min)):
        # 正常出席
        return "○", entry_dt, exit_dt, None

    # (2) 正常出席(○) ② : exit_dt > finish_dt+5分
    if (exit_dt is not None) and (exit_dt > (finish_dt + td_5min)):
        status_str = "○"
        original_exit = exit_dt
        updated_exit_dt = finish_dt
        # 次コマ entry = finish_dt+10分
        next_entry_dt = finish_dt + td_10min
        # 次コマ exit = original_exit
        next_exit_dt  = original_exit
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # (2) 正常出席(○) ③ : exit_dt is None
    if exit_dt is None:
        status_str = "○"
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt  = None
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # (3) 早退 (△早)
    #   entry <= start+5分 かつ exit < finish-5分
    #   xx分 = finish_dt - exit_dt
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt < (finish_dt - td_5min)):
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        return f"△早{delta_min}分", entry_dt, exit_dt, None

    # (4) 遅刻 (△遅)
    #   entry_dt > start_dt+5分 かつ exit_dt <= finish_dt+5分
    #   xx分 = entry_dt - start_dt
    if (entry_dt > (start_dt + td_5min)) and (exit_dt <= (finish_dt + td_5min)):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分", entry_dt, exit_dt, None

    # いずれにも該当しない場合は "？"
    return "？", entry_dt, exit_dt, None

# ---------------------
# メイン処理
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

    # シート書き込み用: (student_index, コース順, 日付文字列) -> 出席ステータス
    results_dict = {}

    # -------------
    # 各学生ごとにループ
    # -------------
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

        # enrollment からコースID取得 (例: "1, 2")
        enroll_path = f"Students/enrollment/student_index/{student_index}"
        enrollment_data = get_data_from_firebase(enroll_path)
        if not enrollment_data or "course_id" not in enrollment_data:
            continue
        course_id_str = enrollment_data["course_id"]
        course_id_list = [x.strip() for x in course_id_str.split(",") if x.strip()]
        if not course_id_list:
            continue

        # attendance_dict 内の entry/exit をペア化
        # 例: [('entry1','exit1'),('entry2','exit2'), ...]
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
            # 1つも entry がなければスキップ (or 全コース欠席)
            continue

        # 最初の entry_dt を base_date として取得
        first_entry_key, _ = entry_exit_pairs[0]
        first_entry_str = attendance_dict[first_entry_key].get("read_datetime", "")
        first_entry_dt = parse_datetime(first_entry_str)
        if not first_entry_dt:
            # データ不備
            continue
        base_date = first_entry_dt.date()  # 例: 2025-01-06 (date型)

        pair_index = 0

        # ======================================
        # コース順にループ: enumerate() で順番idxを取る
        # ======================================
        for idx, c_id_str in enumerate(course_id_list, start=1):
            # idx は「この学生が持つコースの並び順」 (1,2,3,...)
            try:
                c_id_int = int(c_id_str)
            except:
                # 数値変換失敗時はスキップ
                continue
            if c_id_int <= 0 or c_id_int >= len(courses_data):
                continue

            course_info = courses_data[c_id_int]
            schedule_info = course_info.get("schedule", {})
            time_range_str = schedule_info.get("time", "")
            if not time_range_str:
                continue

            # 現在の pair_index が entry_exit_pairs を超えている場合 → 欠席扱い
            if pair_index >= len(entry_exit_pairs):
                absent_date = base_date.strftime("%Y-%m-%d")
                results_dict[(student_index, idx, absent_date)] = "×"
                continue

            # entry/exitキー取得
            ekey, xkey = entry_exit_pairs[pair_index]
            pair_index += 1

            # ローカル attendance_dict から取得 (Firebase再取得しない)
            entry_rec = attendance_dict.get(ekey, {})
            exit_rec  = attendance_dict.get(xkey, {})
            entry_dt_str = entry_rec.get("read_datetime")
            exit_dt_str  = exit_rec.get("read_datetime")

            entry_dt = parse_datetime(entry_dt_str) if entry_dt_str else None
            exit_dt  = parse_datetime(exit_dt_str) if exit_dt_str else None

            if not entry_dt:
                # entryが無ければ欠席
                absent_date = base_date.strftime("%Y-%m-%d")
                results_dict[(student_index, idx, absent_date)] = "×"
                continue

            # コースの開始/終了日時 = base_date + time
            start_t, finish_t = parse_hhmm_range(time_range_str)
            if not start_t or not finish_t:
                continue
            start_dt = combine_date_and_time(base_date, start_t)
            finish_dt = combine_date_and_time(base_date, finish_t)

            # 出欠判定(①②③)
            status, new_entry_dt, new_exit_dt, next_course_data = judge_attendance_for_course(
                entry_dt, exit_dt, start_dt, finish_dt
            )

            # Firebase に書き込む更新用
            updates = {}

            # entry_dt更新
            if new_entry_dt and (new_entry_dt != entry_dt):
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_rec.get("serial_number", "")
                }
                # ローカル attendance_dict を更新し、次コースループで使う
                attendance_dict[ekey] = updates[ekey]

            # exit_dt更新
            if new_exit_dt and (new_exit_dt != exit_dt):
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_rec.get("serial_number", "")
                }
                # ローカル attendance_dict を更新
                attendance_dict[xkey] = updates[xkey]

            # ② or ③ で next_course_data がある(= (next_entry_dt, next_exit_dt))
            if next_course_data:
                next_ekey = f"entry{pair_index+1}"
                next_xkey = f"exit{pair_index+1}"
                next_e, next_x = next_course_data

                # entry2
                if next_e is not None:
                    e_val = {
                        "read_datetime": next_e.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": entry_rec.get("serial_number", "")
                    }
                    updates[next_ekey] = e_val
                    # ローカル attendance_dict にも入れる
                    attendance_dict[next_ekey] = e_val

                # exit2
                if next_x is not None:
                    x_val = {
                        "read_datetime": next_x.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": exit_rec.get("serial_number", "")
                    }
                    updates[next_xkey] = x_val
                    # ローカル attendance_dict にも入れる
                    attendance_dict[next_xkey] = x_val

                # entry_exit_pairs に 新しい (entry{n+1}, exit{n+1}) を追加
                entry_exit_pairs.append((next_ekey, next_xkey))

            # Firebase更新
            if updates:
                att_path = f"Students/attendance/student_id/{student_id}"
                update_data_in_firebase(att_path, updates)

            # シート書き込み結果を格納
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
            print(f"シート({sheet_id})を開けません: {e}")
            continue

        # 当該 student_index の結果のみ取り出す
        std_result_items = {
            k: v for k, v in results_dict.items() if k[0] == std_idx
        }
        if not std_result_items:
            continue

        # 書き込み
        for (s_idx, course_idx, date_str), status_val in std_result_items.items():
            # date_str -> シート名 & 日付取得
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # %d

            # Worksheet
            try:
                ws = sh.worksheet(yyyymm)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            # ★ row と column を逆にする
            #   row = コース順 (course_idx) +1
            #   col = 日付(=day) +1
            row = course_idx + 1
            col = day + 1

            try:
                ws.update_cell(row, col, status_val)
            except Exception as e:
                print(f"シート書き込み失敗 [{sheet_id}:{yyyymm} R{row}C{col}]: {e}")

    print("=== 出席判定とシート書き込みが完了しました ===")

# ---------------------
# 実行部分
# ---------------------
if __name__ == "__main__":
    process_attendance_and_write_sheet()
