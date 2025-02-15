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
    cred = credentials.Certificate("/tmp/firebase_service_account.json")
    firebase_admin.initialize_app(
        cred,
        {
            "databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/",
        },
    )
else:
    print("[DEBUG] Firebaseはすでに初期化済です。")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
print("[DEBUG] Google認証の設定を行います...")
creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/gcp_service_account.json", scope)
gclient = gspread.authorize(creds)
print("[DEBUG] Google認証が完了しました。")


def get_data_from_firebase(path):
    """
    Firebase Realtime Database からデータを取得します。
    """
    print(f"[DEBUG] get_data_from_firebase: {path}")
    ref = db.reference(path)
    data = ref.get()
    print(f"[DEBUG]  -> 取得データ: {data}")
    return data


def update_data_in_firebase(path, data_dict):
    """
    dict形式のデータを update() でまとめて更新します。
    """
    print(f"[DEBUG] update_data_in_firebase: {path} に {data_dict} をupdateします。")
    ref = db.reference(path)
    ref.update(data_dict)


def set_data_in_firebase(path, value):
    """
    単一の値を set() で更新します。
    """
    print(f"[DEBUG] set_data_in_firebase: {path} に {value} をsetします。")
    ref = db.reference(path)
    ref.set(value)


def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    """
    文字列を datetime にパースします。失敗した場合は None を返します。
    """
    if not dt_str:
        return None
    try:
        dt = datetime.datetime.strptime(dt_str, fmt)
        return dt
    except Exception as e:
        print(f"[DEBUG] parse_datetime: 変換失敗 ({dt_str}) {e}")
        return None


def combine_date_and_time(date_dt, time_obj):
    """
    date部分と time部分を合体して datetime を作ります。
    例: 2025-01-01 と 09:30 を合体 → 2025-01-01 09:30:00
    """
    return datetime.datetime(
        date_dt.year,
        date_dt.month,
        date_dt.day,
        time_obj.hour,
        time_obj.minute,
        time_obj.second,
    )


def parse_hhmm(hhmm_str):
    """
    'HH:MM' を datetime.time に変換します。
    """
    hh, mm = map(int, hhmm_str.split(":"))
    return datetime.time(hh, mm, 0)


# period ごとの開始～終了時刻
PERIOD_TIME_MAP = {
    1: ("08:50", "10:20"),
    2: ("10:30", "12:00"),
    3: ("13:10", "14:40"),
    4: ("14:50", "16:20"),
}


def judge_attendance_for_period(entry_dt, exit_dt, start_dt, finish_dt):
    """
    1コマ分の出席判定を行い、ステータスと修正後の入退室時刻、次コマ用データを返します。
    """
    td_5min = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    if entry_dt and entry_dt >= finish_dt:
        # 欠席
        return "×", entry_dt, exit_dt, None

    if (
        entry_dt
        and exit_dt
        and entry_dt <= (start_dt + td_5min)
        and exit_dt < (finish_dt - td_5min)
    ):
        # 早退
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        return f"△早{delta_min}分", entry_dt, exit_dt, None

    # 出席(○)のパターン
    # 1. 時間内に入退室が収まる
    if (
        entry_dt
        and exit_dt
        and entry_dt <= (start_dt + td_5min)
        and exit_dt <= (finish_dt + td_5min)
    ):
        return "〇", entry_dt, exit_dt, None

    # 2. 授業時間を超えても退出していない or 次コマにまたがる
    if (
        entry_dt
        and exit_dt
        and entry_dt <= (start_dt + td_5min)
        and exit_dt >= (finish_dt + td_5min)
    ):
        # 入室はOKだが、終了時間を超えている
        original_exit = exit_dt
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt = original_exit

        # exitが授業時間終了前であれば「△早◯分」になる場合もある
        # ただしここは (exit_dt >= finish_dt + 5min) なので「△早」は該当しないパターン
        return "〇", entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # 2.2 (3-2.2 と3-2.3がやや重複しているが、ロジックを変えず保持)
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
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        return f"△早{delta_min}分", entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    if (
        entry_dt
        and exit_dt
        and entry_dt >= (start_dt + td_5min)
        and exit_dt >= (finish_dt + td_5min)
    ):
        # 遅刻 + 次コマへまたがる
        original_exit = exit_dt
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt = original_exit
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分", entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    if entry_dt and (exit_dt is None):
        # 退出時刻が未登録(居残り)
        status_str = "〇"
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt = None
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # 遅刻
    if (
        entry_dt
        and exit_dt
        and entry_dt > (start_dt + td_5min)
        and exit_dt <= (finish_dt + td_5min)
    ):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分", entry_dt, exit_dt, None

    # 上記以外は不明扱い
    return "？", entry_dt, exit_dt, None


def process_attendance_and_write_sheet():
    """
    出席情報を判定し、Googleスプレッドシートへ結果を書き込みます。
    """
    now = datetime.datetime.now()
    current_weekday_str = now.strftime("%A")
    print(f"[DEBUG] 現在の曜日: {current_weekday_str}")

    # attendance_data の取得
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

    print("[DEBUG] === 学生ごとのループを開始します。===")
    for student_id, att_dict in attendance_data.items():
        if not isinstance(att_dict, dict):
            print(f"[DEBUG] student_id={student_id} の attendance_data が辞書ではありません。スキップします。")
            continue

        # student_index の取得
        print(f"[DEBUG] student_id={student_id} に対応するstudent_indexを取得")
        si_map = student_info_data.get("student_id", {})
        if student_id not in si_map:
            print(f"[DEBUG] student_id={student_id} が student_info_dataに見つからず。スキップします。")
            continue

        student_index = si_map[student_id].get("student_index")
        if not student_index:
            print(f"[DEBUG] student_id={student_id} の student_indexが空。スキップします。")
            continue

        # enrollment (course_id一覧)
        print(f"[DEBUG] enrollment_data から student_index={student_index} を取得します。")
        enroll_info = enrollment_data_all.get(student_index)
        if not enroll_info or "course_id" not in enroll_info:
            print(f"[DEBUG] enrollment_info が存在しないか、 'course_id'がありません。スキップします。")
            continue

        enrolled_course_str = enroll_info["course_id"]
        enrolled_course_ids = [c.strip() for c in enrolled_course_str.split(",") if c.strip()]
        print(f"[DEBUG] student_index={student_index} が履修しているコース: {enrolled_course_ids}")

        # 当日の曜日に合致するコースを抽出 (periodでソート)
        valid_course_list = []
        for cid_str in enrolled_course_ids:
            try:
                cid_int = int(cid_str)
            except ValueError:
                print(f"[DEBUG] cid_str={cid_str} は数値に変換できません。スキップ。")
                continue

            if cid_int < 0 or cid_int >= len(courses_all):
                print(f"[DEBUG] cid_int={cid_int} が courses_all の範囲外です。スキップ。")
                continue

            course_info = courses_all[cid_int]
            if not course_info:
                print(f"[DEBUG] course_id={cid_int} のデータがありません。スキップ。")
                continue

            sched = course_info.get("schedule", {})
            day_in_course = sched.get("day", "")
            period_in_course = sched.get("period", 0)

            if day_in_course == current_weekday_str:
                valid_course_list.append((period_in_course, cid_int))

        valid_course_list.sort(key=lambda x: x[0])
        print(f"[DEBUG] => 曜日({current_weekday_str})が一致する(ソート後)コース: {valid_course_list}")

        # entry/exitの日付取得
        base_date = None
        for i in range(1, 5):
            ekey = f"entry{i}"
            if ekey in att_dict:
                dt_tmp = parse_datetime(att_dict[ekey].get("read_datetime", ""))
                if dt_tmp:
                    base_date = dt_tmp.date()
                    break

        if not base_date:
            print(f"[DEBUG] student_id={student_id} に entry1~entry4のいずれも存在せず。スキップします。")
            continue

        date_str = base_date.strftime("%Y-%m-%d")
        print(f"[DEBUG] => student_id={student_id} / 基準日: {date_str}")

        # period昇順で処理
        for new_course_idx, (period_in_course, cid_int) in enumerate(valid_course_list, start=1):
            course_info = courses_all[cid_int]
            sched = course_info.get("schedule", {})

            if not (1 <= period_in_course <= 4):
                print(f"[DEBUG] course_id={cid_int} の period={period_in_course} は1~4ではないのでスキップ。")
                continue

            ekey = f"entry{period_in_course}"
            xkey = f"exit{period_in_course}"
            print(f"[DEBUG] 判定対象: course_id={cid_int}, period={period_in_course} -> ekey={ekey}, xkey={xkey}")

            if ekey not in att_dict:
                print(f"[DEBUG] {ekey} が無いため欠席(×)扱いとします。")
                status = "×"
                decision_path = f"Students/attendance/student_id/{student_id}/course_id/{cid_int}/decision"
                set_data_in_firebase(decision_path, status)
                results_dict[(student_index, new_course_idx, date_str, cid_int)] = status
                continue

            entry_info = att_dict.get(ekey, {})
            exit_info = att_dict.get(xkey, {})
            entry_dt = parse_datetime(entry_info.get("read_datetime", ""))
            exit_dt = parse_datetime(exit_info.get("read_datetime", ""))

            if period_in_course in PERIOD_TIME_MAP:
                start_hhmm_str, finish_hhmm_str = PERIOD_TIME_MAP[period_in_course]
                start_t = parse_hhmm(start_hhmm_str)
                finish_t = parse_hhmm(finish_hhmm_str)
                start_dt = combine_date_and_time(base_date, start_t)
                finish_dt = combine_date_and_time(base_date, finish_t)
            else:
                print(f"[DEBUG] period={period_in_course} が PERIOD_TIME_MAP に無いのでスキップ。")
                continue

            # 出席判定
            status, new_entry_dt, new_exit_dt, next_period_data = judge_attendance_for_period(
                entry_dt, exit_dt, start_dt, finish_dt
            )
            print(f"[DEBUG] => 判定結果: {status}")

            updates = {}
            if new_entry_dt and (new_entry_dt != entry_dt):
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_info.get("serial_number", ""),
                }
                att_dict[ekey] = updates[ekey]

            if new_exit_dt and exit_dt and (new_exit_dt != exit_dt):
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_info.get("serial_number", ""),
                }
                att_dict[xkey] = updates[xkey]
            elif new_exit_dt and exit_dt is None:
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_info.get("serial_number", ""),
                }
                att_dict[xkey] = updates[xkey]

            if next_period_data:
                next_ekey = f"entry{period_in_course + 1}"
                next_xkey = f"exit{period_in_course + 1}"
                next_e, next_x = next_period_data
                print(f"[DEBUG] 次コマ用データを作成します。 entry={next_e}, exit={next_x}")
                if next_e:
                    updates[next_ekey] = {
                        "read_datetime": next_e.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": entry_info.get("serial_number", ""),
                    }
                    att_dict[next_ekey] = updates[next_ekey]
                if next_x:
                    updates[next_xkey] = {
                        "read_datetime": next_x.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": exit_info.get("serial_number", ""),
                    }
                    att_dict[next_xkey] = updates[next_xkey]

            if updates:
                att_path = f"Students/attendance/student_id/{student_id}"
                update_data_in_firebase(att_path, updates)

            decision_path = f"Students/attendance/student_id/{student_id}/course_id/{cid_int}/decision"
            set_data_in_firebase(decision_path, status)
            results_dict[(student_index, new_course_idx, date_str, cid_int)] = status

    print("[DEBUG] === シート書き込み処理を開始します。===")
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

        std_result_items = {k: v for k, v in results_dict.items() if k[0] == std_idx}
        if not std_result_items:
            print(f"[DEBUG] student_index={std_idx} に該当する出席判定結果がありません。スキップします。")
            continue

        enroll_info = enrollment_data_all.get(std_idx)
        if not enroll_info or "course_id" not in enroll_info:
            print(f"[DEBUG] student_index={std_idx} の enrollment_data がありません。スキップします。")
            continue

        enrolled_course_str = enroll_info["course_id"]
        enrolled_course_ids = [c.strip() for c in enrolled_course_str.split(",") if c.strip()]

        print(f"[DEBUG] -> student_index={std_idx} への書き込み対象: {std_result_items}")
        for (s_idx, new_course_idx, date_str, cid_int), status_val in std_result_items.items():
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # "dd"

            try:
                ws = sh.worksheet(yyyymm)
                print(f"[DEBUG] 既存のワークシート {yyyymm} を取得しました。")
            except gspread.exceptions.WorksheetNotFound:
                print(f"[DEBUG] ワークシート {yyyymm} が見つからないため、新規作成します。")
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            cid_str = str(cid_int)
            try:
                course_pos = enrolled_course_ids.index(cid_str)
            except ValueError:
                print(f"[DEBUG] cid_str={cid_str} が enrolled_course_ids に見つからないためスキップします。")
                continue

            row = course_pos + 2
            col = day + 1

            print(f"[DEBUG] シート[{yyyymm}] (row={row}, col={col}) に '{status_val}' を書き込みます。")
            ws.update_cell(row, col, status_val)

    print("=== 出席判定処理＆シート書き込み完了 ===")


if __name__ == "__main__":
    process_attendance_and_write_sheet()
