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
    if not dt_str:
        return None
    try:
        return datetime.datetime.strptime(dt_str, fmt)
    except:
        return None

def parse_hhmm_range(range_str):
    if not range_str:
        return None, None
    try:
        start_str, end_str = range_str.split("~")
        sh, sm = map(int, start_str.split(":"))
        eh, em = map(int, end_str.split(":"))
        return datetime.time(sh, sm, 0), datetime.time(eh, em, 0)
    except:
        return None, None

def combine_date_and_time(date_dt, time_obj):
    return datetime.datetime(
        date_dt.year, date_dt.month, date_dt.day,
        time_obj.hour, time_obj.minute, time_obj.second
    )

# ---------------------
# 出席判定ロジック
# ---------------------
def judge_attendance_for_course(entry_dt, exit_dt, start_dt, finish_dt):
    td_5min  = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    # (1) 欠席(×)
    if entry_dt >= finish_dt:
        return "×", entry_dt, exit_dt, None

    # (2) 早退(△早)
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt < (finish_dt - td_5min)):
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        return f"△早{delta_min}分", entry_dt, exit_dt, None

    # (3) 遅刻(△遅)
    if (entry_dt > (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt <= (finish_dt + td_5min)):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分", entry_dt, exit_dt, None

    # (4) 正常(○) ①
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt <= (finish_dt + td_5min)):
        return "○", entry_dt, exit_dt, None

    # (4) 正常(○) ②: exit > finish+5分
    if (exit_dt is not None) and (exit_dt > (finish_dt + td_5min)):
        status_str = "○"
        original_exit = exit_dt
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt  = original_exit
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # (4) 正常(○) ③: exit=None
    if exit_dt is None:
        status_str = "○"
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt = None
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # その他
    return "？", entry_dt, exit_dt, None


# ---------------------
# メインフロー
# ---------------------
def process_attendance_and_write_sheet():
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("attendance データがありません。終了します。")
        return

    courses_data = get_data_from_firebase("Courses/course_id")
    student_info_data = get_data_from_firebase("Students/student_info")

    results_dict = {}  # {(student_index, new_course_idx, date_str): status}

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

        # enrollment
        enroll_path = f"Students/enrollment/student_index/{student_index}"
        enrollment_data = get_data_from_firebase(enroll_path)
        if not enrollment_data or "course_id" not in enrollment_data:
            continue
        course_id_str = enrollment_data["course_id"]
        course_id_list = [x.strip() for x in course_id_str.split(",") if x.strip()]
        if not course_id_list:
            continue

        # entry/exit ペア取得
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
            continue

        # 最初の entry_dt
        first_entry_key, _ = entry_exit_pairs[0]
        first_entry_str = attendance_dict[first_entry_key].get("read_datetime", "")
        first_entry_dt = parse_datetime(first_entry_str)
        if not first_entry_dt:
            continue

        base_date = first_entry_dt.date()
        # エントリー時点の曜日
        #   例: "Monday", "Tuesday", ...
        entry_weekday_str = first_entry_dt.strftime("%A")

        # -----------------
        # 1) 曜日をチェックし、スキップされなかったコースだけを valid_courses に集める
        # -----------------
        valid_courses = []
        for c_id_str in course_id_list:
            try:
                c_id_int = int(c_id_str)
            except:
                continue
            if c_id_int <= 0 or c_id_int >= len(courses_data):
                continue

            course_info = courses_data[c_id_int]
            schedule_info = course_info.get("schedule", {})
            time_range_str = schedule_info.get("time", "")
            day_str = schedule_info.get("day", "")  # 例: "Monday"

            # 曜日が不一致ならスキップ
            if day_str and (day_str != entry_weekday_str):
                # day_strが空の場合は特にスキップしない、としている
                continue

            # time_rangeが無いならスキップ
            if not time_range_str:
                continue

            # validコースリストに追加
            #   (c_id_int, time_range_str, day_str) など必要なものを入れておく
            valid_courses.append((c_id_int, time_range_str, day_str))

        # もし全スキップされて valid_coursesが空なら何もしない
        if not valid_courses:
            continue

        # pair_index (entry_exit_pairsの何番目か)
        pair_index = 0

        # -----------------
        # 2) valid_courses を new_course_idx で回す
        # -----------------
        for new_course_idx, (c_id_int, time_range_str, day_str) in enumerate(valid_courses, start=1):
            # pair_index が足りなければ欠席扱いする
            if pair_index >= len(entry_exit_pairs):
                # ペア不足
                absent_date = base_date.strftime("%Y-%m-%d")
                results_dict[(student_index, new_course_idx, absent_date)] = "×"
                continue

            # entry/exit 取得
            ekey, xkey = entry_exit_pairs[pair_index]
            pair_index += 1

            entry_rec = attendance_dict.get(ekey, {})
            exit_rec  = attendance_dict.get(xkey, {})
            entry_dt_str = entry_rec.get("read_datetime")
            exit_dt_str  = exit_rec.get("read_datetime")

            entry_dt = parse_datetime(entry_dt_str)
            exit_dt  = parse_datetime(exit_dt_str)

            if not entry_dt:
                # entry無 → 欠席
                absent_date = base_date.strftime("%Y-%m-%d")
                results_dict[(student_index, new_course_idx, absent_date)] = "×"
                continue

            # コース開始/終了
            start_t, finish_t = parse_hhmm_range(time_range_str)
            if not start_t or not finish_t:
                continue

            start_dt = combine_date_and_time(base_date, start_t)
            finish_dt = combine_date_and_time(base_date, finish_t)

            status, new_entry_dt, new_exit_dt, next_course_data = judge_attendance_for_course(
                entry_dt, exit_dt, start_dt, finish_dt
            )

            updates = {}
            # entry更新
            if new_entry_dt and (new_entry_dt != entry_dt):
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_rec.get("serial_number", "")
                }
                attendance_dict[ekey] = updates[ekey]

            # exit更新
            if new_exit_dt and (new_exit_dt != exit_dt):
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_rec.get("serial_number", "")
                }
                attendance_dict[xkey] = updates[xkey]

            # 次コマ作成(② or ③)
            if next_course_data:
                next_ekey = f"entry{pair_index+1}"
                next_xkey = f"exit{pair_index+1}"
                next_e, next_x = next_course_data
                if next_e:
                    updates[next_ekey] = {
                        "read_datetime": next_e.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": entry_rec.get("serial_number", "")
                    }
                    attendance_dict[next_ekey] = updates[next_ekey]
                if next_x:
                    updates[next_xkey] = {
                        "read_datetime": next_x.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": exit_rec.get("serial_number", "")
                    }
                    attendance_dict[next_xkey] = updates[next_xkey]

                # ローカルペアに追加
                entry_exit_pairs.append((next_ekey, next_xkey))

            if updates:
                att_path = f"Students/attendance/student_id/{student_id}"
                update_data_in_firebase(att_path, updates)

            date_str = base_date.strftime("%Y-%m-%d")
            # 結果を results_dict に格納
            results_dict[(student_index, new_course_idx, date_str)] = status

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
        except:
            continue

        std_result_items = {
            k: v for k, v in results_dict.items() if k[0] == std_idx
        }
        if not std_result_items:
            continue

        for (s_idx, new_course_idx, date_str), status_val in std_result_items.items():
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # (%d)
            try:
                ws = sh.worksheet(yyyymm)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            # 行 = new_course_idx +1
            # 列 = 日付 +1
            row = new_course_idx + 1
            col = day + 1
            ws.update_cell(row, col, status_val)

    print("=== 出席判定処理＆シート書き込み完了 ===")


if __name__ == "__main__":
    process_attendance_and_write_sheet()
