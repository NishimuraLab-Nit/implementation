import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ---------------------
# Firebase & GSpread初期化
# ---------------------
if not firebase_admin._apps:
    print("[DEBUG] Firebase未初期化。credentials.Certificateを使用して初期化します...")
    cred = credentials.Certificate("/tmp/firebase_service_account.json")  # ここは適宜書き換え
    firebase_admin.initialize_app(
        cred,
        {
            "databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/",  # ここは適宜書き換え
        },
    )
else:
    print("[DEBUG] Firebaseはすでに初期化済です。")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
print("[DEBUG] Google認証の設定を行います...")
creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/gcp_service_account.json", scope)  # ここは適宜書き換え
gclient = gspread.authorize(creds)
print("[DEBUG] Google認証が完了しました。")


def get_data_from_firebase(path):
    print(f"[DEBUG] get_data_from_firebase: {path}")
    ref = db.reference(path)
    data = ref.get()
    print(f"[DEBUG]  -> 取得データ: {data}")
    return data


def update_data_in_firebase(path, data_dict):
    print(f"[DEBUG] update_data_in_firebase: {path} に {data_dict} をupdateします。")
    ref = db.reference(path)
    ref.update(data_dict)


def set_data_in_firebase(path, value):
    print(f"[DEBUG] set_data_in_firebase: {path} に {value} をsetします。")
    ref = db.reference(path)
    ref.set(value)


def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    if not dt_str:
        return None
    try:
        return datetime.datetime.strptime(dt_str, fmt)
    except Exception as e:
        print(f"[DEBUG] parse_datetime: 変換失敗 ({dt_str}) {e}")
        return None


def combine_date_and_time(date_dt, time_obj):
    return datetime.datetime(
        date_dt.year,
        date_dt.month,
        date_dt.day,
        time_obj.hour,
        time_obj.minute,
        time_obj.second,
    )


def parse_hhmm(hhmm_str):
    hh, mm = map(int, hhmm_str.split(":"))
    return datetime.time(hh, mm, 0)


# period(1～4)ごとの開始～終了時刻
PERIOD_TIME_MAP = {
    1: ("08:50", "10:20"),
    2: ("10:30", "12:00"),
    3: ("13:10", "14:40"),
    4: ("14:50", "16:20"),
}


def judge_attendance_for_period(entry_dt, exit_dt, start_dt, finish_dt):
    """
    1コマ分の出席判定を行い、ステータスと修正後の入退室時刻、次コマ用データを返す。
    """
    td_5min = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    # ---------------------------
    # 【修正】entry はあるが exit が無い場合
    # ---------------------------
    if entry_dt and (exit_dt is None):
        if entry_dt >= (start_dt + td_5min):
            delta_min = int((entry_dt - start_dt).total_seconds() // 60)
            return f"△遅{delta_min}分", entry_dt, None, None  # 4タプルにする
        return "〇", entry_dt, None, None
        
    # 入室が授業終了後 → 欠席
    if entry_dt and entry_dt >= finish_dt:
        return "×", entry_dt, exit_dt, None

    # 早退
    if (
        entry_dt
        and exit_dt
        and entry_dt <= (start_dt + td_5min)
        and exit_dt < (finish_dt - td_5min)
    ):
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        return f"△早{delta_min}分", entry_dt, exit_dt, None

    # 通常の出席(〇)パターン: 時間内に入退室が収まる
    if (
        entry_dt
        and exit_dt
        and entry_dt <= (start_dt + td_5min)
        and exit_dt <= (finish_dt + td_5min)
    ):
        return "〇", entry_dt, exit_dt, None

    # 〇パターン: 次コマにまたがる (退室が授業終了後 etc.)
    if (
        entry_dt
        and exit_dt
        and entry_dt <= (start_dt + td_5min)
        and exit_dt >= (finish_dt + td_5min)
    ):
        original_exit = exit_dt
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt = original_exit
        return "〇", entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # 遅刻 + 次コマへまたがる
    if (
        entry_dt
        and exit_dt
        and entry_dt >= (start_dt + td_5min)
        and exit_dt >= (finish_dt + td_5min)
    ):
        original_exit = exit_dt
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt = original_exit
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分", entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # 遅刻（exit はあるが entry が遅れている etc.）
    if (
        entry_dt
        and exit_dt
        and entry_dt > (start_dt + td_5min)
        and exit_dt <= (finish_dt + td_5min)
    ):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分", entry_dt, exit_dt, None

    # 上記いずれにも該当しないケース → 「？」(不明)
    return "？", entry_dt, exit_dt, None


def ensure_slot_is_free(att_dict, updates, slot_idx):
    """
    slot_idx番 (entry{slot_idx}/exit{slot_idx}) が既に使われている場合、
    後ろ(最大4まで)へずらして空きを作り、実際に使えるスロット番号を返す。

    - ずらした内容も 'updates' に反映しておき、後で Firebase に update() する。
    - 1日最大4スロット前提。
    """
    if slot_idx > 4:
        return 4

    ekey = f"entry{slot_idx}"
    xkey = f"exit{slot_idx}"

    # 空いていれば、そのスロットを返す
    if (ekey not in att_dict) and (xkey not in att_dict):
        return slot_idx

    # slot_idx=4 まで埋まっているなら、上書きするしかない
    if slot_idx == 4:
        return 4

    # ここに来たなら、slot_idx < 4 かつ埋まっている
    next_slot = ensure_slot_is_free(att_dict, updates, slot_idx + 1)
    ekey_next = f"entry{next_slot}"
    xkey_next = f"exit{next_slot}"

    # 現在のスロットにあるデータを next_slot に移動
    if ekey in att_dict:
        updates[ekey_next] = att_dict[ekey]  # updates にも記録
        att_dict[ekey_next] = att_dict[ekey]
        del att_dict[ekey]
    if xkey in att_dict:
        updates[xkey_next] = att_dict[xkey]
        att_dict[xkey_next] = att_dict[xkey]
        del att_dict[xkey]

    # この slot_idx が空いたので、それを返す
    return slot_idx


def process_attendance_and_write_sheet():
    now = datetime.datetime.now()
    current_weekday_str = now.strftime("%A")
    print(f"[DEBUG] 現在の曜日: {current_weekday_str}")

    print("[DEBUG] attendance_data を取得します。")
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("attendance データがありません。終了します。")
        return

    print("[DEBUG] Courses/course_id を取得します。")
    courses_all = get_data_from_firebase("Courses/course_id")

    print("[DEBUG] Students/student_info を取得します。")
    student_info_data = get_data_from_firebase("Students/student_info")

    print("[DEBUG] Students/enrollment/student_index を取得します。")
    enrollment_data_all = get_data_from_firebase("Students/enrollment/student_index")

    if not courses_all or not student_info_data or not enrollment_data_all:
        print("[DEBUG] 必要なデータが不足しています。終了します。")
        return

    results_dict = {}

    print("[DEBUG] === 学生ごとのループを開始します。 ===")
    for student_id, att_dict in attendance_data.items():
        if not isinstance(att_dict, dict):
            continue

        # student_index を取得
        si_map = student_info_data.get("student_id", {})
        if student_id not in si_map:
            continue

        student_index = si_map[student_id].get("student_index")
        if not student_index:
            continue

        # enrollment (course_id一覧)
        enroll_info = enrollment_data_all.get(student_index)
        if not enroll_info or "course_id" not in enroll_info:
            continue

        enrolled_course_str = enroll_info["course_id"]
        enrolled_course_ids = [c.strip() for c in enrolled_course_str.split(",") if c.strip()]
        print(f"[DEBUG] student_index={student_index} が履修しているコース: {enrolled_course_ids}")

        # 今日の曜日に合致するコースを抽出し、(period, course_id) でソート
        valid_course_list = []
        for cid_str in enrolled_course_ids:
            try:
                cid_int = int(cid_str)
            except ValueError:
                continue
            if cid_int < 0 or cid_int >= len(courses_all):
                continue

            course_info = courses_all[cid_int]
            if not course_info:
                continue

            sched = course_info.get("schedule", {})
            day_in_course = sched.get("day", "")
            period_in_course = sched.get("period", 0)
            # 今日の曜日と合致するコースのみ
            if day_in_course == current_weekday_str:
                valid_course_list.append((period_in_course, cid_int))

        # (period_in_course, cid_int) でソート => period が小さい順、同じなら course_id が小さい順
        valid_course_list.sort(key=lambda x: (x[0], x[1]))
        print(f"[DEBUG] => 当日対象のコース一覧(sorted): {valid_course_list}")

        # 基準日(最初に見つかった entry1～entry4 の read_datetime の日付)
        base_date = None
        for i in range(1, 5):
            ekey_test = f"entry{i}"
            if ekey_test in att_dict:
                dt_tmp = parse_datetime(att_dict[ekey_test].get("read_datetime", ""))
                if dt_tmp:
                    base_date = dt_tmp.date()
                    break

        if not base_date:
            print(f"[DEBUG] student_id={student_id} に entry1～4 の日時が無くスキップ")
            continue

        date_str = base_date.strftime("%Y-%m-%d")
        print(f"[DEBUG] => student_id={student_id} / 基準日: {date_str}")

        # valid_course_list の順に、1コース目→entry1, 2コース目→entry2,…で処理
        for new_course_idx, (schedule_period, cid_int) in enumerate(valid_course_list, start=1):
            if not (1 <= schedule_period <= 4):
                continue

            ekey = f"entry{new_course_idx}"
            xkey = f"exit{new_course_idx}"

            print(f"[DEBUG] => course_id={cid_int}, period={schedule_period} -> ekey={ekey}, xkey={xkey}")

            if ekey and (xkey is None):
                if ekey not in att_dict:
                    # entry が無ければ欠席扱い
                    print(f"[DEBUG] {ekey} が無いので欠席(×)")
                    status = "×"
                    decision_path = f"Students/attendance/student_id/{student_id}/course_id/{cid_int}/decision"
                    set_data_in_firebase(decision_path, status)
                    results_dict[(student_index, new_course_idx, date_str, cid_int)] = status
                    continue

            entry_info = att_dict[ekey]
            exit_info = att_dict.get(xkey, {})
            entry_dt = parse_datetime(entry_info.get("read_datetime", ""))
            exit_dt = parse_datetime(exit_info.get("read_datetime", ""))

            start_hhmm_str, finish_hhmm_str = PERIOD_TIME_MAP[schedule_period]
            start_t = parse_hhmm(start_hhmm_str)
            finish_t = parse_hhmm(finish_hhmm_str)
            start_dt = combine_date_and_time(base_date, start_t)
            finish_dt = combine_date_and_time(base_date, finish_t)

            # 出席判定
            status, new_entry_dt, new_exit_dt, next_period_data = judge_attendance_for_period(
                entry_dt, exit_dt, start_dt, finish_dt
            )
            print(f"[DEBUG] => 判定結果: {status}")

            # Firebase に書き込むデータをまとめる
            updates = {}

            # 入室時刻の補正
            if new_entry_dt and (new_entry_dt != entry_dt):
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_info.get("serial_number", ""),
                }
                att_dict[ekey] = updates[ekey]

            # 退出時刻の補正
            if new_exit_dt and (new_exit_dt != exit_dt):
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_info.get("serial_number", ""),
                }
                att_dict[xkey] = updates[xkey]
            elif new_exit_dt and not exit_dt:
                # exit_dt が無い時に新規で書き込む場合
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_info.get("serial_number", ""),
                }
                att_dict[xkey] = updates[xkey]

            # 次コマへまたがる場合
            if next_period_data and new_course_idx < 4:
                next_e, next_x = next_period_data

                # 本来 entry{new_course_idx+1} を使うが、そこが埋まっていれば後ろにずらす
                slot_for_next = ensure_slot_is_free(att_dict, updates, new_course_idx + 1)
                next_ekey = f"entry{slot_for_next}"
                next_xkey = f"exit{slot_for_next}"
                print(f"[DEBUG] 次コマデータを slot={slot_for_next} に書き込み (entry={next_e}, exit={next_x})")

                if next_e:
                    updates[next_ekey] = {
                        "read_datetime": next_e.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": entry_info.get("serial_number", ""),  # 同じカードIDとして扱う例
                    }
                    att_dict[next_ekey] = updates[next_ekey]

                if next_x:
                    updates[next_xkey] = {
                        "read_datetime": next_x.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": exit_info.get("serial_number", ""),
                    }
                    att_dict[next_xkey] = updates[next_xkey]

            # updates があれば Firebase に反映
            if updates:
                att_path = f"Students/attendance/student_id/{student_id}"
                update_data_in_firebase(att_path, updates)

            # コースごとの判定結果を記録
            decision_path = f"Students/attendance/student_id/{student_id}/course_id/{cid_int}/decision"
            set_data_in_firebase(decision_path, status)
            results_dict[(student_index, new_course_idx, date_str, cid_int)] = status

    print("[DEBUG] === シート書き込み処理を開始します。 ===")
    all_student_index_data = student_info_data.get("student_index", {})
    for std_idx, info_val in all_student_index_data.items():
        sheet_id = info_val.get("sheet_id")
        if not sheet_id:
            print(f"[DEBUG] student_index={std_idx} に sheet_id がありません。スキップ。")
            continue

        try:
            print(f"[DEBUG] Google SpreadSheetを開きます: sheet_id={sheet_id}")
            sh = gclient.open_by_key(sheet_id)
        except Exception as e:
            print(f"[DEBUG] シートを開けませんでした。例外: {e}")
            continue

        # 結果を書き込む対象のみ抽出
        std_result_items = {k: v for k, v in results_dict.items() if k[0] == std_idx}
        if not std_result_items:
            continue

        # enrollment情報から、この学生が履修しているcourse_id一覧を取得
        enroll_info = enrollment_data_all.get(std_idx)
        if not enroll_info or "course_id" not in enroll_info:
            continue

        enrolled_course_str = enroll_info["course_id"]
        enrolled_course_ids = [c.strip() for c in enrolled_course_str.split(",") if c.strip()]

        for (s_idx, new_course_idx, date_str, cid_int), status_val in std_result_items.items():
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # "dd"

            # 該当シートを開く or 新規作成
            try:
                ws = sh.worksheet(yyyymm)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            cid_str = str(cid_int)
            try:
                course_pos = enrolled_course_ids.index(cid_str)
            except ValueError:
                continue

            # (行, 列)は例としてコース順+2, 日付+1
            row = course_pos + 2
            col = day + 1
            ws.update_cell(row, col, status_val)

    print("=== 出席判定処理＆シート書き込み完了 ===")


if __name__ == "__main__":
    process_attendance_and_write_sheet()
