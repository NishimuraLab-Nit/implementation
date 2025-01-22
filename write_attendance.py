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
    try:
        return datetime.datetime.strptime(dt_str, fmt)
    except Exception:
        return None

def parse_hhmm_range(range_str):
    """ "8:50~10:20" → (time(8,50), time(10,20)) """
    try:
        start_str, end_str = range_str.split("~")
        sh, sm = map(int, start_str.split(":"))
        eh, em = map(int, end_str.split(":"))
        return datetime.time(sh, sm, 0), datetime.time(eh, em, 0)
    except:
        return None, None

def combine_date_and_time(date_dt, time_obj):
    """date_dt (日付) + time_obj(時刻) → datetime."""
    return datetime.datetime(
        date_dt.year, date_dt.month, date_dt.day,
        time_obj.hour, time_obj.minute, time_obj.second
    )

# ---------------------
# 出欠判定ロジック
# ---------------------
def judge_attendance_for_course(entry_dt, exit_dt, start_dt, finish_dt):
    """
    仕様に沿った出欠判定:
      - 欠席(×), 正常(○), 早退(△早), 遅刻(△遅)
      - exit > finish+5分 の場合 exitをfinishに補正、次コマentryを finish+10分
      - exit が None の場合 exitをfinishに補正
    戻り値: ( status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt )
    """
    status_str = ""
    updated_entry_dt = entry_dt
    updated_exit_dt = exit_dt
    next_course_entry_dt = None

    td_5min  = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    # (1) 欠席 → entryが終了時刻以降の場合
    if entry_dt >= finish_dt:
        return "×", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # (2) 早退(△早)
    #     entry <= start+5分 かつ exit < finish-5分
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt < (finish_dt - td_5min)):
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        return f"△早{delta_min}分", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # (3) 遅刻(△遅)
    #     entry > start+5分 かつ exit <= finish+5分
    if (entry_dt > (start_dt + td_5min)) and (exit_dt <= (finish_dt + td_5min)):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # (4) 正常出席(○)
    #     1) entry <= start+5分 かつ exit <= finish+5分
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt <= (finish_dt + td_5min)):
        return "○", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    #     2) exit > finish+5分
    if exit_dt > (finish_dt + td_5min):
        status_str = "○"
        updated_exit_dt = finish_dt
        # 次コマentryは finish+10分 だが、日付は同一に揃える
        temp_dt = finish_dt + td_10min
        forced_next_dt = datetime.datetime(
            finish_dt.year, finish_dt.month, finish_dt.day,
            temp_dt.hour, temp_dt.minute, temp_dt.second
        )
        next_course_entry_dt = forced_next_dt
        return status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt

    #     3) exitがNone -> finishに補正
    if exit_dt is None:
        status_str = "○"
        updated_exit_dt = finish_dt
        return status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # それ以外は一旦 "？"
    return "？", updated_entry_dt, updated_exit_dt, next_course_entry_dt

# ---------------------
# メインフロー
# ---------------------
def process_attendance_and_write_sheet():
    """
    1) Students/attendance/student_id/ の各 student_id でエントリー/エグジット取得
    2) enrollment(Students/enrollment/student_index/xxx) から受講コースID取得
    3) コースのschedule(time) と entry/exit を比較 → 出欠判定
    4) シートに書き込む: シート名= "YYYY-MM", 行= (コースID+1), 列= (日+1)
       ※ exit2などの作成時に日付がズレても、シート上は「最初のコース日」に固定する
    """
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("出席データがありません")
        return

    courses_data = get_data_from_firebase("Courses/course_id")  # 0番目: None想定
    student_info_data = get_data_from_firebase("Students/student_info")

    results_dict = {}  # {(student_index, course_id, yyyymmdd) : status}

    for student_id, attendance_dict in attendance_data.items():
        if not isinstance(attendance_dict, dict):
            continue

        # student_index取得
        student_index = None
        if (student_info_data.get("student_id")
            and student_id in student_info_data["student_id"]
            and "student_index" in student_info_data["student_id"][student_id]):
            student_index = student_info_data["student_id"][student_id]["student_index"]
        if not student_index:
            continue

        # enrollment からコースIDを取得
        enroll_path = f"Students/enrollment/student_index/{student_index}"
        enrollment_data = get_data_from_firebase(enroll_path)
        if not enrollment_data or "course_id" not in enrollment_data:
            continue
        course_id_str = enrollment_data["course_id"]  # "1, 2"など
        course_id_list = [x.strip() for x in course_id_str.split(",") if x.strip()]
        if not course_id_list:
            continue

        # entry/exit ペアを収集
        entry_exit_pairs = []
        i = 1
        while True:
            ekey = f"entry{i}"
            xkey = f"exit{i}"
            if ekey not in attendance_dict:
                break
            entry_exit_pairs.append((ekey, xkey))
            i += 1

        # ----------------------------------------
        # (★) まず最初の entry_dt を取得して「この日のベース日付」を確定
        # ----------------------------------------
        if entry_exit_pairs:
            first_entry_key, _ = entry_exit_pairs[0]
            first_entry_dt_str = attendance_dict[first_entry_key].get("read_datetime", "")
            first_entry_dt = parse_datetime(first_entry_dt_str) if first_entry_dt_str else None
        else:
            # entryが一つもないならスキップ
            continue

        if not first_entry_dt:
            # 最初のentryが無い → このstudent_idはデータ不備なのでスキップ
            continue

        # この人の「当日」を固定
        base_date = datetime.date(
            first_entry_dt.year,
            first_entry_dt.month,
            first_entry_dt.day
        )
        # 例えば 2025-01-06
        # これをシート書き込みやコース時間計算の「日付」として使う

        # exit2 作成などで日付が翌日にズレても、シート上は base_date で固定
        # ----------------------------------------

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
            time_range_str = schedule_info.get("time", "")  # 例: "8:50~10:20"
            if not time_range_str:
                continue

            if pair_index >= len(entry_exit_pairs):
                # entry/exit ペア不足 → 欠席(×)
                # シートに書き込む日付は base_date
                date_str = base_date.strftime("%Y-%m-%d")
                results_dict[(student_index, c_id, date_str)] = "×"
                continue

            ekey, xkey = entry_exit_pairs[pair_index]
            pair_index += 1

            entry_rec = attendance_dict.get(ekey, {})
            exit_rec  = attendance_dict.get(xkey, {})
            entry_dt_str = entry_rec.get("read_datetime")
            exit_dt_str  = exit_rec.get("read_datetime")
            entry_dt = parse_datetime(entry_dt_str) if entry_dt_str else None
            exit_dt  = parse_datetime(exit_dt_str) if exit_dt_str else None

            if not entry_dt:
                # entry が無ければ欠席扱い
                date_str = base_date.strftime("%Y-%m-%d")
                results_dict[(student_index, c_id, date_str)] = "×"
                continue

            # コース予定時刻
            start_t, finish_t = parse_hhmm_range(time_range_str)
            if not start_t or not finish_t:
                continue

            # ------ ここがポイント ------
            # コースの開始・終了を「base_dateの日付＋HH:MM」として作る
            start_dt = combine_date_and_time(base_date, start_t)
            finish_dt = combine_date_and_time(base_date, finish_t)
            # → こうすると、翌日に行っても計算上は同日のXX:XXとして比較できる

            # 出欠判定
            status, new_entry_dt, new_exit_dt, next_course_entry_dt = judge_attendance_for_course(
                entry_dt, exit_dt, start_dt, finish_dt
            )

            # Firebase更新 (entry/exit)
            updates = {}
            if new_entry_dt and new_entry_dt != entry_dt:
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_rec.get("serial_number", "")
                }
            if new_exit_dt and new_exit_dt != exit_dt:
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_rec.get("serial_number", "")
                }

            # 次コマentryを作成
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

            # シート書き込み結果を記録
            # ※ ここでは必ず base_date を使用し、シート上の列を固定
            sheet_date_str = base_date.strftime("%Y-%m-%d")
            results_dict[(student_index, c_id, sheet_date_str)] = status

    # ==============================
    # シート更新
    # ==============================
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

        # 当該 student_index 分の結果を取得
        std_result_items = {
            k: v for k, v in results_dict.items() if k[0] == std_idx
        }
        if not std_result_items:
            continue

        for (s_idx, c_id, date_str), status_val in std_result_items.items():
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # %d

            # シート(ワークシート)を用意
            try:
                ws = sh.worksheet(yyyymm)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            row = c_id + 1  # コースIDが1開始
            col = day + 1   # 日付 +1
            try:
                ws.update_cell(row, col, status_val)
            except Exception as e:
                print(f"書き込み失敗 {sheet_id}:{yyyymm} R{row}C{col}: {e}")

    print("=== 出席判定とシート書き込みが完了しました ===")

if __name__ == "__main__":
    process_attendance_and_write_sheet()
