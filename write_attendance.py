import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Firebase & GSpread初期化
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gclient = gspread.authorize(creds)

def get_data_from_firebase(path):
    ref = db.reference(path)
    return ref.get()

def update_data_in_firebase(path, data_dict):
    ref = db.reference(path)
    ref.update(data_dict)

# ---------------------
# 日時関連ユーティリティ
# ---------------------
def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    try:
        return datetime.datetime.strptime(dt_str, fmt)
    except Exception:
        return None

def parse_hhmm_range(range_str):
    """
    "8:50~10:20" のような文字列を (datetime.time, datetime.time) にパースして返す。
    """
    try:
        start_str, end_str = range_str.split("~")
        sh, sm = map(int, start_str.split(":"))
        eh, em = map(int, end_str.split(":"))
        return datetime.time(sh, sm, 0), datetime.time(eh, em, 0)
    except:
        return None, None

def combine_date_and_time(date_dt, time_obj):
    """date_dtの日付 + time_objの時刻を合わせて datetime を返す。"""
    return datetime.datetime(
        date_dt.year, date_dt.month, date_dt.day,
        time_obj.hour, time_obj.minute, time_obj.second
    )

# ---------------------
# 出欠判定ロジック（修正部分あり）
# ---------------------
def judge_attendance_for_course(entry_dt, exit_dt, start_dt, finish_dt):
    """
    # 修正内容:
    #   exit が finish+5分 以降の場合、
    #     exit=finish に強制して 次コマの entry=finish+10分 とするが、
    #     そのとき「日付が繰り上がらない」ように修正。
    """

    status_str = ""
    updated_entry_dt = entry_dt
    updated_exit_dt = exit_dt
    next_course_entry_dt = None

    td_5min = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    # 欠席判定
    if entry_dt >= finish_dt:
        return "×", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # 早退(△早)
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt < (finish_dt - td_5min)):
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        return f"△早{delta_min}分", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # 遅刻(△遅)
    if (entry_dt > (start_dt + td_5min)) and (exit_dt <= (finish_dt + td_5min)):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # 正常出席(○)パターン
    # (1) entry <= start+5分 かつ exit <= finish+5分
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt <= (finish_dt + td_5min)):
        return "○", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # (2) exit > finish+5分
    if exit_dt > (finish_dt + td_5min):
        status_str = "○"  # このコマは正常扱い
        original_exit_dt = exit_dt
        # exitをfinish_dtに
        updated_exit_dt = finish_dt

        # ---- 修正ここから ----
        # 次コマentry = finish_dt + 10分 だが、日付(年月日)を finish_dt と同じに固定
        temp_dt = finish_dt + td_10min
        forced_next_dt = datetime.datetime(
            finish_dt.year,
            finish_dt.month,
            finish_dt.day,
            temp_dt.hour,
            temp_dt.minute,
            temp_dt.second
        )
        next_course_entry_dt = forced_next_dt
        # ---- 修正ここまで ----

        return status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # (3) exitがNoneなら exit=finish_dt
    if exit_dt is None:
        status_str = "○"
        updated_exit_dt = finish_dt
        return status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # 上記パターン以外は一旦 "？"
    return "？", updated_entry_dt, updated_exit_dt, next_course_entry_dt

# ---------------------
# メインの処理フロー
# ---------------------
def process_attendance_and_write_sheet():
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("attendance データなし")
        return

    courses_data = get_data_from_firebase("Courses/course_id")  # 0番目: None
    student_info_data = get_data_from_firebase("Students/student_info")

    results_dict = {}  # (student_index, course_id, date_str) => 出席ステータス

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

        # enrollment からコースID取得
        enroll_path = f"Students/enrollment/student_index/{student_index}"
        enrollment_data = get_data_from_firebase(enroll_path)
        if not enrollment_data or "course_id" not in enrollment_data:
            continue
        course_id_str = enrollment_data["course_id"]  # "1, 2" など
        course_id_list = [x.strip() for x in course_id_str.split(",") if x.strip()]

        # entry/exitペアをまとめる
        entry_exit_pairs = []
        i = 1
        while True:
            ekey = f"entry{i}"
            xkey = f"exit{i}"
            if ekey not in attendance_dict:
                break
            entry_exit_pairs.append((ekey, xkey))
            i += 1

        pair_index = 0
        for c_id_str in course_id_list:
            try:
                c_id = int(c_id_str)
            except:
                continue
            if c_id <= 0 or c_id >= len(courses_data):
                continue
            course_info = courses_data[c_id]
            schedule_info = course_info.get("schedule", {})
            time_range_str = schedule_info.get("time", "")  # "8:50~10:20"など
            if not time_range_str:
                continue

            if pair_index >= len(entry_exit_pairs):
                # entry/exitが無い → 欠席
                today_str = datetime.datetime.now().strftime("%Y-%m-%d")
                results_dict[(student_index, c_id, today_str)] = "×"
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
                # entryなし → 欠席
                today_str = datetime.datetime.now().strftime("%Y-%m-%d")
                results_dict[(student_index, c_id, today_str)] = "×"
                continue

            # time_rangeを datetime に
            start_t, finish_t = parse_hhmm_range(time_range_str)
            if not start_t or not finish_t:
                continue

            start_dt  = combine_date_and_time(entry_dt, start_t)
            finish_dt = combine_date_and_time(entry_dt, finish_t)

            # 出欠判定
            status, new_entry_dt, new_exit_dt, next_course_entry_dt = judge_attendance_for_course(
                entry_dt, exit_dt, start_dt, finish_dt
            )

            # Firebase更新
            updates = {}
            if new_entry_dt != entry_dt:
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_rec.get("serial_number", "")
                }
            if new_exit_dt != exit_dt and new_exit_dt is not None:
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_rec.get("serial_number", "")
                }

            # 次コマ用 entry がある場合
            if next_course_entry_dt:
                next_ekey = f"entry{pair_index+1}"  # 次コマのentryキー想定
                next_xkey = f"exit{pair_index+1}"   # 次コマのexitキー想定(暫定)
                # exitの引き継ぎなどしたい場合は original_exit_dt を別で持ち回す
                updates[next_ekey] = {
                    "read_datetime": next_course_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_rec.get("serial_number", "")
                }
                if exit_dt:
                    # exitは「前のoriginal_exit」など
                    updates[next_xkey] = {
                        "read_datetime": exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": exit_rec.get("serial_number", "")
                    }

            if updates:
                att_path = f"Students/attendance/student_id/{student_id}"
                update_data_in_firebase(att_path, updates)

            # シート書き込み用にステータスを保持
            date_str = entry_dt.strftime("%Y-%m-%d")
            results_dict[(student_index, c_id, date_str)] = status

    # -----------------
    # シート更新
    # -----------------
    all_student_index_data = student_info_data.get("student_index", {})
    for std_idx, info_val in all_student_index_data.items():
        sheet_id = info_val.get("sheet_id")
        if not sheet_id:
            continue

        try:
            sh = gclient.open_by_key(sheet_id)
        except Exception as e:
            print(f"シートID={sheet_id} にアクセスできません: {e}")
            continue

        # 学生std_idxに紐づく結果のみ
        std_result_items = {
            k: v for k, v in results_dict.items() if k[0] == std_idx
        }
        if not std_result_items:
            continue

        # 結果書き込み
        for (s_idx, c_id, date_str), status_val in std_result_items.items():
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # %d

            try:
                ws = sh.worksheet(yyyymm)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            row = c_id + 1  # コースIDが1始まりという前提の例
            col = day + 1   # 日付(%d) +1 の例

            try:
                ws.update_cell(row, col, status_val)
            except Exception as e:
                print(f"シート書き込み失敗 [{sheet_id}:{yyyymm}({row},{col})]: {e}")

    print("=== 出席判定処理完了 ===")

if __name__ == "__main__":
    process_attendance_and_write_sheet()
