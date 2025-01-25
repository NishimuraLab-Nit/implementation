import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==============================
# Firebase & GSpread 初期化
# ==============================
if not firebase_admin._apps:
    print("[DEBUG] Firebase未初期化。credentials.Certificateを使用して初期化します...")
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })
else:
    print("[DEBUG] Firebaseはすでに初期化済です。")

scope = ["https://spreadsheets.google.com/feeds", 
         "https://www.googleapis.com/auth/drive"]
print("[DEBUG] Google認証の設定を行います...")
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gclient = gspread.authorize(creds)
print("[DEBUG] Google認証が完了しました。")

# ==============================
# Firebaseアクセス関連
# ==============================
def get_data_from_firebase(path):
    print(f"[DEBUG] get_data_from_firebase: {path}")
    ref = db.reference(path)
    data = ref.get()
    print(f"[DEBUG]  -> 取得データ: {data}")
    return data

def update_data_in_firebase(path, data_dict):
    """ dict形式のデータを update() でまとめて更新 """
    print(f"[DEBUG] update_data_in_firebase: {path} に {data_dict} をupdateします。")
    ref = db.reference(path)
    ref.update(data_dict)

def set_data_in_firebase(path, value):
    """ 単一の値(文字列など)を set() で更新 """
    print(f"[DEBUG] set_data_in_firebase: {path} に {value} をsetします。")
    ref = db.reference(path)
    ref.set(value)

# ==============================
# ユーティリティ
# ==============================
def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    if not dt_str:
        return None
    try:
        dt = datetime.datetime.strptime(dt_str, fmt)
        return dt
    except Exception as e:
        print(f"[DEBUG] parse_datetime: 変換失敗 ({dt_str}) {e}")
        return None

def parse_hhmm(hhmm_str):
    """ 'HH:MM' を datetime.time にするユーティリティ """
    hh, mm = map(int, hhmm_str.split(":"))
    return datetime.time(hh, mm, 0)

def combine_date_and_time(date_dt, time_obj):
    return datetime.datetime(
        date_dt.year, date_dt.month, date_dt.day,
        time_obj.hour, time_obj.minute, time_obj.second
    )

# ==============================
# 出席判定ロジック
# ==============================
def judge_attendance_for_period(entry_dt, exit_dt, start_dt, finish_dt):
    """
    仕様:
      (1) 欠席(×):
         - entry_dt >= finish_dt

      (2) 早退(△早):
         - entry_dt <= start_dt+5分
         - exit_dt < finish_dt-5分

      (3) 出席(〇):
         (3-1) entry_dt <= start_dt+5分 & exit_dt <= finish_dt+5分
         (3-2) [②のケース]
               entry_dt <= start_dt+5分 & exit_dt >= finish_dt+5分
                 -> exit1をfinish1に更新
                 -> 次コマ作成 (entry2=finish1+10分, exit2=元exit1)
                 -> period=1は"〇"を記録、次period(2)へ移行
         (3-3) [③のケース]
               exit_dt が無い(None)
                 -> exit1=finish1
                 -> 次コマ作成 (entry2=finish1+10分, exit2=None)
                 -> period=1は"〇"を記録

      (4) 遅刻(△遅):
         - entry_dt > start_dt+5分
         - exit_dt <= finish_dt+5分

      (5) その他(？)

    戻り値: (status_str, new_entry_dt, new_exit_dt, (next_entry_dt, next_exit_dt))
    """

    import datetime
    td_5min  = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    print("[DEBUG] ------------------------------")
    print("[DEBUG] judge_attendance_for_period 呼び出し")
    print(f"[DEBUG]  entry_dt={entry_dt}, exit_dt={exit_dt}")
    print(f"[DEBUG]  start_dt={start_dt}, finish_dt={finish_dt}")

    # (1) 欠席 (×)
    if entry_dt and entry_dt >= finish_dt:
        print("[DEBUG] -> 欠席(×)")
        return "×", entry_dt, exit_dt, None

    # (2) 早退 (△早)
    if (entry_dt and exit_dt
        and entry_dt <= (start_dt + td_5min)
        and exit_dt <  (finish_dt - td_5min)):
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        print(f"[DEBUG] -> 早退(△早{delta_min}分)")
        return f"△早{delta_min}分", entry_dt, exit_dt, None

    # (3) 出席 (〇)
    #  (3-1) entry_dt <= start+5分 かつ exit_dt <= finish+5分
    if (entry_dt and exit_dt
        and entry_dt <= (start_dt + td_5min)
        and exit_dt <= (finish_dt + td_5min)):
        print("[DEBUG] -> 出席(〇) - 通常ケース(3-1)")
        return "〇", entry_dt, exit_dt, None

    #  (3-2) [②] entry1 が start1+5分以内 かつ exit1 が finish1+5分以降
    #         exit1をfinish1に書き換え、
    #         entry2=finish1+10分, exit2=元exit1
    if (entry_dt and exit_dt
        and entry_dt <= (start_dt + td_5min)
        and exit_dt >= (finish_dt + td_5min)):
        print("[DEBUG] -> 出席(〇) - (3-2) exit_dt >= finish+5分; 次コマ生成")
        status_str = "〇"  # period=1は〇とする
        # いったん元のexitを退避
        original_exit = exit_dt
        # exit1をfinish1に上書き
        updated_exit_dt = finish_dt
        # 新たな次コマの entry2/exit2
        next_entry_dt = finish_dt + td_10min
        next_exit_dt  = original_exit
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    #  (3-3) [③] もしexit1が存在しない(None) なら、
    #         exit1=finish1, entry2=finish1+10分, exit2=None
    if (entry_dt and exit_dt is None):
        print("[DEBUG] -> 出席(〇) - (3-3) exit_dt=None; finish_dtを代入し次コマ生成")
        status_str = "〇"
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt  = None
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # (4) 遅刻 (△遅)
    if (entry_dt and exit_dt
        and entry_dt > (start_dt + td_5min)
        and exit_dt <= (finish_dt + td_5min)):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        print(f"[DEBUG] -> 遅刻(△遅{delta_min}分)")
        return f"△遅{delta_min}分", entry_dt, exit_dt, None

    # (5) その他 (？)
    print("[DEBUG] -> ？ (その他)")
    return "？", entry_dt, exit_dt, None


# ==============================
# メインフロー
# ==============================
def process_attendance_and_write_sheet():
    now = datetime.datetime.now()
    current_weekday_str = now.strftime("%A")
    print(f"[DEBUG] 現在の曜日: {current_weekday_str}")

    # =======================
    # Firebaseデータ取得
    # =======================
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("[DEBUG] attendance_data がありません。終了します。")
        return

    courses_all = get_data_from_firebase("Courses/course_id")
    student_info_data = get_data_from_firebase("Students/student_info")
    enrollment_data_all = get_data_from_firebase("Students/enrollment/student_index")

    if not (courses_all and student_info_data and enrollment_data_all):
        print("[DEBUG] いずれかのデータが不足しています。終了します。")
        return

    # {(student_index, new_period_idx, date_str): status} 用
    results_dict = {}

    print("[DEBUG] === 学生ごとのループ開始 ===")
    for student_id, att_dict in attendance_data.items():
        if not isinstance(att_dict, dict):
            continue

        # student_index 取得
        si_map = student_info_data.get("student_id", {})
        if student_id not in si_map:
            print(f"[DEBUG] student_id={student_id} が student_info_dataに無いのでスキップ")
            continue
        student_index = si_map[student_id].get("student_index")
        if not student_index:
            print(f"[DEBUG] student_id={student_id} の student_indexが空。スキップ。")
            continue

        # enrollmentデータ
        enroll_info = enrollment_data_all.get(student_index)
        if not enroll_info or "course_id" not in enroll_info:
            print(f"[DEBUG] student_index={student_index} enrollmentが不完全。スキップ。")
            continue

        enrolled_course_str = enroll_info["course_id"]
        enrolled_course_ids = [x.strip() for x in enrolled_course_str.split(",") if x.strip()]
        print(f"[DEBUG] student_index={student_index} 履修コース: {enrolled_course_ids}")

        # 当日の最初に存在する entryX の日付を基準日とする
        base_date = None
        for i in range(1, 10):
            # 1～9くらいまで見ておく（必要に応じて拡大）
            ekey = f"entry{i}"
            if ekey in att_dict:
                dt_tmp = parse_datetime(att_dict[ekey].get("read_datetime", ""))
                if dt_tmp:
                    base_date = dt_tmp.date()
                    break
        if not base_date:
            print(f"[DEBUG] student_id={student_id} に entry1～entry9が見つからずスキップ")
            continue

        date_str = base_date.strftime("%Y-%m-%d")
        print(f"[DEBUG] => student_id={student_id}, 基準日: {date_str}")

        # 1) 曜日一致するコースを抽出 → (cid_int, schedule_period) で一時リスト
        valid_list = []
        for cid_str in enrolled_course_ids:
            try:
                cid_int = int(cid_str)
            except:
                continue
            if cid_int < 0 or cid_int >= len(courses_all):
                continue

            cinfo = courses_all[cid_int]
            if not cinfo:
                continue

            sched = cinfo.get("schedule", {})
            day_in_course = sched.get("day", "")
            db_period     = sched.get("period", 0)
            if day_in_course == current_weekday_str:
                # その日の対象コースとする
                valid_list.append((cid_int, db_period))

        # DBに記録された period で昇順ソート
        #  例: (course_id=3, period=1), (course_id=2, period=2), (course_id=1, period=3), ...
        valid_list.sort(key=lambda x: x[1])
        print("[DEBUG] => 該当コース(ソート後):", valid_list)

        # new_period_idx = 1 からスタートして entry1/exit1, entry2/exit2... を割り当てる
        for new_period_idx, (cid_int, db_period) in enumerate(valid_list, start=1):
            print(f"[DEBUG] ---- period={new_period_idx} (DB上はperiod={db_period}), course_id={cid_int} ----")

            course_info = courses_all[cid_int]
            schedule_info = course_info.get("schedule", {})
            time_str = schedule_info.get("time", "")  # "08:50~10:20" など

            ekey = f"entry{new_period_idx}"
            xkey = f"exit{new_period_idx}"

            if ekey not in att_dict:
                print(f"[DEBUG] {ekey} が無いため欠席扱い(×)")
                status = "×"
                decision_path = f"Students/attendance/student_id/{student_id}/course_id/{cid_int}/decision"
                set_data_in_firebase(decision_path, status)
                results_dict[(student_index, new_period_idx, date_str)] = status
                continue

            # entry/exit datetime取得
            entry_info = att_dict.get(ekey, {})
            exit_info  = att_dict.get(xkey, {})
            entry_dt = parse_datetime(entry_info.get("read_datetime", ""))
            exit_dt  = parse_datetime(exit_info.get("read_datetime", ""))

            # schedule.time をパース
            if "~" in time_str:
                s_part, f_part = time_str.split("~")
                start_t = parse_hhmm(s_part)
                finish_t = parse_hhmm(f_part)
            else:
                # もし time が見つからない場合のデフォルト
                start_t = datetime.time(8, 50, 0)
                finish_t = datetime.time(10, 20, 0)
            start_dt = combine_date_and_time(base_date, start_t)
            finish_dt = combine_date_and_time(base_date, finish_t)

            # 出席判定
            status, new_entry_dt, new_exit_dt, next_period_data = judge_attendance_for_period(
                entry_dt, exit_dt, start_dt, finish_dt
            )
            print(f"[DEBUG] => 判定結果: {status}")

            # 必要に応じて entry/exit の時刻を更新
            updates = {}
            if new_entry_dt and new_entry_dt != entry_dt:
                print(f"[DEBUG] entry_dt 更新: {entry_dt} -> {new_entry_dt}")
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_info.get("serial_number", "")
                }
                att_dict[ekey] = updates[ekey]
            if new_exit_dt and new_exit_dt != exit_dt:
                print(f"[DEBUG] exit_dt 更新: {exit_dt} -> {new_exit_dt}")
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_info.get("serial_number", "")
                }
                att_dict[xkey] = updates[xkey]

            # 次コマ用 entry/exit を新規作成( exit>finish+5分 の場合など )
            if next_period_data:
                next_e, next_x = next_period_data
                next_ekey = f"entry{new_period_idx + 1}"
                next_xkey = f"exit{new_period_idx + 1}"
                print(f"[DEBUG] 次コマ生成: {next_ekey}, {next_xkey}")
                if next_e:
                    updates[next_ekey] = {
                        "read_datetime": next_e.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": entry_info.get("serial_number", "")
                    }
                    att_dict[next_ekey] = updates[next_ekey]
                if next_x:
                    updates[next_xkey] = {
                        "read_datetime": next_x.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": exit_info.get("serial_number", "")
                    }
                    att_dict[next_xkey] = updates[next_xkey]

            # Firebase更新
            if updates:
                att_path = f"Students/attendance/student_id/{student_id}"
                update_data_in_firebase(att_path, updates)

            # 出席判定結果を Firebaseに格納
            decision_path = f"Students/attendance/student_id/{student_id}/course_id/{cid_int}/decision/{date_str}"
            set_data_in_firebase(decision_path, status)

            # シート書き込み用に保存
            results_dict[(student_index, new_period_idx, date_str)] = status

    # =============================
    # シート書き込み
    # =============================
    print("[DEBUG] === シート書き込み処理を開始 ===")
    student_index_map = student_info_data.get("student_index", {})
    for sidx, info_val in student_index_map.items():
        sheet_id = info_val.get("sheet_id")
        if not sheet_id:
            print(f"[DEBUG] student_index={sidx} に sheet_id が無いのでスキップ")
            continue
        try:
            print(f"[DEBUG] シートを開きます: sheet_id={sheet_id}")
            sh = gclient.open_by_key(sheet_id)
        except Exception as e:
            print(f"[DEBUG] シートを開けませんでした: {e}")
            continue

        # 当該 student_index に対応する result を集める
        filtered = {(k[1], k[2]): v 
                    for k,v in results_dict.items() 
                    if k[0] == sidx}
        if not filtered:
            print(f"[DEBUG] student_index={sidx} に該当する結果がありません。スキップ。")
            continue

        # 書き込み実行
        print(f"[DEBUG] -> student_index={sidx} の書き込み対象: {filtered}")
        for (new_period_idx, date_str), status_val in filtered.items():
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # DD
            try:
                ws = sh.worksheet(yyyymm)
                print(f"[DEBUG] 既存ワークシート '{yyyymm}' を取得しました。")
            except gspread.exceptions.WorksheetNotFound:
                print(f"[DEBUG] WS '{yyyymm}' が無いので新規作成します。")
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            row = new_period_idx + 1
            col = day + 1
            print(f"[DEBUG] シート[{yyyymm}] (row={row}, col={col}) に '{status_val}' を書き込み")
            ws.update_cell(row, col, status_val)

    print("=== 出席判定処理＆シート書き込み完了 ===")


if __name__ == "__main__":
    process_attendance_and_write_sheet()
