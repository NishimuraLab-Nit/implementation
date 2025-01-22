import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------
# Firebase & GSpread初期化
# ---------------------
if not firebase_admin._apps:
    print("Firebase初期化します...")
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })
    print("Firebase初期化完了。")

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
print("Googleスプレッドシート認証します...")
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gclient = gspread.authorize(creds)
print("Googleスプレッドシート認証完了。")

# ---------------------
# Firebaseアクセス関連
# ---------------------
def get_data_from_firebase(path):
    print(f"Firebaseから取得: {path}")
    ref = db.reference(path)
    data = ref.get()
    print(f"取得データ: {data}")
    return data

def update_data_in_firebase(path, data_dict):
    print(f"Firebase更新: {path}, 更新内容: {data_dict}")
    ref = db.reference(path)
    ref.update(data_dict)
    print("Firebase更新完了。")

# ---------------------
# ユーティリティ
# ---------------------
def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    """文字列 → datetime に変換。失敗時は None を返す"""
    if not dt_str:
        return None
    try:
        result = datetime.datetime.strptime(dt_str, fmt)
        return result
    except Exception as e:
        print(f"[DEBUG] parse_datetime失敗: dt_str={dt_str}, e={e}")
        return None

def parse_hhmm_range(range_str):
    """
    "8:50~10:20" のような文字列を (time(8,50), time(10,20)) に変換
    """
    if not range_str:
        return None, None
    try:
        start_str, end_str = range_str.split("~")
        sh, sm = map(int, start_str.split(":"))
        eh, em = map(int, end_str.split(":"))
        return datetime.time(sh, sm, 0), datetime.time(eh, em, 0)
    except Exception as e:
        print(f"[DEBUG] parse_hhmm_range失敗: range_str={range_str}, e={e}")
        return None, None

def combine_date_and_time(date_dt, time_obj):
    """
    date_dt(年月日) + time_obj(時分秒) → 新たな datetime
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
# 出欠判定ロジック(①②③、早退、遅刻、欠席)
# ---------------------
def judge_attendance_for_course(entry_dt, exit_dt, start_dt, finish_dt):
    """
    ロジック:
      # 欠席 (×): entry_dt >= finish_dt
      # 正常(○)①: entry_dt <= start+5分 かつ exit_dt <= finish+5分
      # 正常(○)②: exit_dt > finish+5分 → exit=finish, next_entry=finish+10分, next_exit=original_exit
      # 正常(○)③: exit_dt=None → exit=finish, next_entry=finish+10分
      # 早退(△早): entry_dt <= start+5分 かつ exit_dt < finish-5分
      # 遅刻(△遅): entry_dt > start+5分 かつ exit_dt <= finish+5分
      # 順序は本コードで調整（欠席→正常①②③→早退→遅刻→？）
    """
    print(" --- judge_attendance_for_course --- ")
    print(f"  entry_dt={entry_dt}, exit_dt={exit_dt}")
    print(f"  start_dt={start_dt}, finish_dt={finish_dt}")

    td_5min  = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    # (1) 欠席(×)
    if entry_dt >= finish_dt:
        print("  → 欠席(×)  (entry_dt >= finish_dt)")
        return "×", entry_dt, exit_dt, None

    # (2) 正常(○)①
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt <= (finish_dt + td_5min)):
        print("  → 正常(○)①")
        return "○", entry_dt, exit_dt, None

    # (2) 正常(○)②: exit_dt > finish+5分
    if (exit_dt is not None) and (exit_dt > (finish_dt + td_5min)):
        print("  → 正常(○)② (exit_dt > finish+5分)")
        original_exit = exit_dt
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt  = original_exit
        return "○", entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # (2) 正常(○)③: exit_dt=None
    if exit_dt is None:
        print("  → 正常(○)③ (exit_dt=None)")
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt  = None
        return "○", entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # (3) 早退(△早)
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt < (finish_dt - td_5min)):
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        print(f"  → 早退(△早) {delta_min}分")
        return f"△早{delta_min}分", entry_dt, exit_dt, None

    # (4) 遅刻(△遅)
    if (entry_dt > (start_dt + td_5min)) and (exit_dt <= (finish_dt + td_5min)):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        print(f"  → 遅刻(△遅) {delta_min}分")
        return f"△遅{delta_min}分", entry_dt, exit_dt, None

    # (5) その他 → "？"
    print("  → どれにも該当せず(？)")
    return "？", entry_dt, exit_dt, None


# ---------------------
# メイン処理
# ---------------------
def process_attendance_and_write_sheet():
    print("===== 処理開始: process_attendance_and_write_sheet =====")
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("attendanceデータがありません。終了します。")
        return

    courses_data = get_data_from_firebase("Courses/course_id")  # 0番目: None想定
    student_info_data = get_data_from_firebase("Students/student_info")

    # シート書き込み用: {(student_index, course_idx, date_str): status}
    results_dict = {}

    # -----------------
    # 学生ID単位ループ
    # -----------------
    for student_id, attendance_dict in attendance_data.items():
        print(f"\n[DEBUG] 処理対象の student_id={student_id}")
        if not isinstance(attendance_dict, dict):
            print(" [DEBUG] attendance_dictがdictではありません。スキップ。")
            continue

        # student_index 取得
        student_index = None
        if (student_info_data.get("student_id")
            and student_id in student_info_data["student_id"]
            and "student_index" in student_info_data["student_id"][student_id]):
            student_index = student_info_data["student_id"][student_id]["student_index"]
        if not student_index:
            print(" [DEBUG] student_index取得できず。スキップ。")
            continue
        print(f" [DEBUG] student_index={student_index}")

        # enrollment からコースID取得
        enroll_path = f"Students/enrollment/student_index/{student_index}"
        enrollment_data = get_data_from_firebase(enroll_path)
        if not enrollment_data or "course_id" not in enrollment_data:
            print(" [DEBUG] enrollmentデータ無し or course_idキー無し。スキップ。")
            continue
        course_id_str = enrollment_data["course_id"]
        course_id_list = [x.strip() for x in course_id_str.split(",") if x.strip()]
        if not course_id_list:
            print(" [DEBUG] course_id_listが空です。スキップ。")
            continue
        print(f" [DEBUG] course_id_list={course_id_list}")

        # attendance_dict 内のentry/exitペア抽出
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
            print(" [DEBUG] entry_exit_pairsが空。スキップ。")
            continue
        print(f" [DEBUG] entry_exit_pairs={entry_exit_pairs}")

        # 最初の entry_dtを見て base_date 確定
        first_entry_key, _ = entry_exit_pairs[0]
        first_entry_str = attendance_dict[first_entry_key].get("read_datetime", "")
        first_entry_dt = parse_datetime(first_entry_str)
        if not first_entry_dt:
            print(" [DEBUG] 最初のエントリーの日時が不正。スキップ。")
            continue

        base_date = first_entry_dt.date()
        print(f" [DEBUG] base_date={base_date}")

        pair_index = 0

        # -----------------
        # コースリスト順に処理
        # -----------------
        for idx, c_id_str in enumerate(course_id_list, start=1):
            # idx=1,2,3,...(学生が持つコースの並び順)
            print(f"\n  >> [DEBUG] courseIndex={idx}, course_id_str={c_id_str}")
            try:
                c_id_int = int(c_id_str)
            except:
                print(f"   [DEBUG] コースIDの数値化に失敗。c_id_str={c_id_str}. スキップ。")
                continue
            if c_id_int <= 0 or c_id_int >= len(courses_data):
                print(f"   [DEBUG] c_id_int={c_id_int} が courses_data範囲外。スキップ。")
                continue

            course_info = courses_data[c_id_int]
            schedule_info = course_info.get("schedule", {})
            time_range_str = schedule_info.get("time", "")
            print(f"   [DEBUG] schedule time_range={time_range_str}")

            if not time_range_str:
                print("   [DEBUG] time_range_strが空。スキップ。")
                continue

            if pair_index >= len(entry_exit_pairs):
                print("   [DEBUG] entry_exit_pairsがもう無い→欠席(×)扱い")
                absent_date = base_date.strftime("%Y-%m-%d")
                results_dict[(student_index, idx, absent_date)] = "×"
                continue

            ekey, xkey = entry_exit_pairs[pair_index]
            pair_index += 1
            print(f"   [DEBUG] 今回使用するエントリーペア: (ekey={ekey}, xkey={xkey})")

            # ローカル attendance_dict から 取り出す
            entry_rec = attendance_dict.get(ekey, {})
            exit_rec  = attendance_dict.get(xkey, {})
            entry_dt_str = entry_rec.get("read_datetime")
            exit_dt_str  = exit_rec.get("read_datetime")

            entry_dt = parse_datetime(entry_dt_str)
            exit_dt  = parse_datetime(exit_dt_str)
            print(f"   [DEBUG] entry_dt={entry_dt}, exit_dt={exit_dt}")

            if not entry_dt:
                print("   [DEBUG] entry_dtが無い→欠席(×)")
                absent_date = base_date.strftime("%Y-%m-%d")
                results_dict[(student_index, idx, absent_date)] = "×"
                continue

            # コース開始/終了を作成
            start_t, finish_t = parse_hhmm_range(time_range_str)
            if not start_t or not finish_t:
                print("   [DEBUG] start_t or finish_tが不正。スキップ。")
                continue
            start_dt = combine_date_and_time(base_date, start_t)
            finish_dt = combine_date_and_time(base_date, finish_t)
            print(f"   [DEBUG] start_dt={start_dt}, finish_dt={finish_dt}")

            # 出欠判定
            status, new_entry_dt, new_exit_dt, next_course_data = judge_attendance_for_course(
                entry_dt, exit_dt, start_dt, finish_dt
            )
            print(f"   [DEBUG] status={status}, new_entry_dt={new_entry_dt}, new_exit_dt={new_exit_dt}, next_course_data={next_course_data}")

            updates = {}
            # entry_dt更新
            if new_entry_dt and (new_entry_dt != entry_dt):
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_rec.get("serial_number", "")
                }
                attendance_dict[ekey] = updates[ekey]  # ローカル更新

            # exit_dt更新
            if new_exit_dt and (new_exit_dt != exit_dt):
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_rec.get("serial_number", "")
                }
                attendance_dict[xkey] = updates[xkey]  # ローカル更新

            # next_course_data → (next_entry_dt, next_exit_dt)
            if next_course_data:
                next_ekey = f"entry{pair_index+1}"
                next_xkey = f"exit{pair_index+1}"
                next_e, next_x = next_course_data
                if next_e:
                    e_val = {
                        "read_datetime": next_e.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": entry_rec.get("serial_number", "")
                    }
                    updates[next_ekey] = e_val
                    attendance_dict[next_ekey] = e_val
                if next_x:
                    x_val = {
                        "read_datetime": next_x.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": exit_rec.get("serial_number", "")
                    }
                    updates[next_xkey] = x_val
                    attendance_dict[next_xkey] = x_val

                # ローカル entry_exit_pairs に追加
                entry_exit_pairs.append((next_ekey, next_xkey))
                print(f"   [DEBUG] 新規追加したentry/exitペア=({next_ekey}, {next_xkey})")

            if updates:
                att_path = f"Students/attendance/student_id/{student_id}"
                update_data_in_firebase(att_path, updates)

            # シート書き込み結果を格納
            date_str = base_date.strftime("%Y-%m-%d")
            results_dict[(student_index, idx, date_str)] = status
            print(f"   [DEBUG] (student_index={student_index}, course_idx={idx}, date={date_str}) → {status}")

    # -----------------
    # シート書き込み
    # -----------------
    print("\n=== シート書き込みフェーズ ===")
    all_student_index_data = student_info_data.get("student_index", {})
    for std_idx, info_val in all_student_index_data.items():
        sheet_id = info_val.get("sheet_id")
        if not sheet_id:
            continue

        print(f"\n[DEBUG] シート書き込み対象の student_index={std_idx}, sheet_id={sheet_id}")
        try:
            sh = gclient.open_by_key(sheet_id)
            print(f" [DEBUG] シートオープン成功: {sh.title}")
        except Exception as e:
            print(f" [DEBUG] シート({sheet_id})を開けません: {e}")
            continue

        # 当該 student_index の結果のみ
        std_result_items = {
            k: v for k, v in results_dict.items() if k[0] == std_idx
        }
        if not std_result_items:
            print(" [DEBUG] 書き込み対象データなし。")
            continue

        # 書き込み
        for (s_idx, course_idx, date_str), status_val in std_result_items.items():
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # %d

            try:
                ws = sh.worksheet(yyyymm)
            except gspread.exceptions.WorksheetNotFound:
                print(f" [DEBUG] ワークシート {yyyymm} 不在 → 新規作成")
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            # row=コース順+1, col=日+1
            row = course_idx + 1
            col = day + 1
            print(f" [DEBUG] シート {yyyymm}, row={row}, col={col}, status_val={status_val}")

            try:
                ws.update_cell(row, col, status_val)
            except Exception as e:
                print(f" [DEBUG] シート書き込み失敗: {e}")
            else:
                print(" [DEBUG] シート書き込み成功。")

    print("\n=== 出席判定とシート書き込みが完了しました ===")


# ---------------------
# 実行
# ---------------------
if __name__ == "__main__":
    process_attendance_and_write_sheet()
