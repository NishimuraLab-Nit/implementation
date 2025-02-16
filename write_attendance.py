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
    cred = credentials.Certificate("/tmp/firebase_service_account.json")  # パスを自環境に合わせて
    firebase_admin.initialize_app(
        cred,
        {
            "databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/",  # URLを自環境に合わせて
        },
    )
else:
    print("[DEBUG] Firebaseはすでに初期化済です。")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
print("[DEBUG] Google認証の設定を行います...")
creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/gcp_service_account.json", scope)  # パスを自環境に合わせて
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


# period(1～4)ごとの実際の授業開始～終了時刻
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

    # 入室が授業終了後の場合 → 欠席
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

    # 出席(○)のパターン1: 時間内に入退室が収まる
    if (
        entry_dt
        and exit_dt
        and entry_dt <= (start_dt + td_5min)
        and exit_dt <= (finish_dt + td_5min)
    ):
        return "〇", entry_dt, exit_dt, None

    # (○)のパターン2: 授業終了後も退出していない or 次コマにまたがる
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

    # 退出時刻が未登録(居残り)
    if entry_dt and (exit_dt is None):
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt = None
        return "〇", entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # 遅刻
    if (
        entry_dt
        and exit_dt
        and entry_dt > (start_dt + td_5min)
        and exit_dt <= (finish_dt + td_5min)
    ):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分", entry_dt, exit_dt, None

    # いずれにも該当しない → ？
    return "？", entry_dt, exit_dt, None


def ensure_slot_is_free(att_dict, updates, slot_idx):
    """
    slot_idx 番 (entry{slot_idx}/exit{slot_idx}) が既に使われている場合、
    後ろのスロット(最大4)へ順次ずらして空きを作る。

    - ずらし作業も含め、Firebaseに反映できるように 'updates' にも書き込みを追加する。
    - 1日最大4スロット想定。
    - 例: ensure_slot_is_free(att_dict, updates, 2) を呼び出すと、
        entry2/exit2 が埋まっていれば entry3/exit3 へ移し、
        さらにentry3/exit3が埋まっていればentry4/exit4へ... と順にずらす。
    - 最終的に空いたスロット番号を返す。
    """
    if slot_idx > 4:
        return 4

    ekey = f"entry{slot_idx}"
    xkey = f"exit{slot_idx}"

    # スロットが空いていれば、このスロットを返して終了
    if (ekey not in att_dict) and (xkey not in att_dict):
        return slot_idx

    # もし slot_idx=4 (最後) まで埋まっていれば、やむを得ず上書きするしかないので4を返す
    if slot_idx == 4:
        return 4

    # ここに来たのは「このスロットが埋まっていて、まだ後ろに空きの可能性がある」場合
    # → slot_idx+1 を空ける
    next_slot = ensure_slot_is_free(att_dict, updates, slot_idx + 1)
    ekey_next = f"entry{next_slot}"
    xkey_next = f"exit{next_slot}"

    # slot_idx のデータを next_slot に移動
    if ekey in att_dict:
        updates[ekey_next] = att_dict[ekey]  # updates にも記録
        att_dict[ekey_next] = att_dict[ekey]
        del att_dict[ekey]
    if xkey in att_dict:
        updates[xkey_next] = att_dict[xkey]
        att_dict[xkey_next] = att_dict[xkey]
        del att_dict[xkey]

    # slot_idx を空けることに成功したので、slot_idx を返す
    return slot_idx


def process_attendance_and_write_sheet():
    """
    出席情報を判定し、Firebaseへ更新＆Googleスプレッドシートへ書き込みします。
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

        # 今日の曜日に合致するコースを抽出 (schedule.periodでソート)
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
            if day_in_course == current_weekday_str:
                valid_course_list.append((period_in_course, cid_int))

        valid_course_list.sort(key=lambda x: x[0])
        print(f"[DEBUG] => 曜日({current_weekday_str})が一致するコース: {valid_course_list}")

        # 基準日（最初に見つかった entry1～4 のread_datetime から日付を取得）
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

        # valid_course_listの並び順に従い、1コース目→entry1, 2コース目→entry2,…の形で処理
        for new_course_idx, (schedule_period, cid_int) in enumerate(valid_course_list, start=1):
            if not (1 <= schedule_period <= 4):
                continue

            # entry{n}, exit{n} （n = new_course_idx）
            ekey = f"entry{new_course_idx}"
            xkey = f"exit{new_course_idx}"
            print(f"[DEBUG] course_id={cid_int}, schedule_period={schedule_period} -> ekey={ekey}, xkey={xkey}")

            if ekey not in att_dict:
                # entry が無い → 欠席
                print(f"[DEBUG] {ekey} が無いため欠席(×)扱い")
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

            # 今回コースに対する Firebase の更新内容を貯める
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
                # exit_dt が無い(居残り)→新しく書き込み
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_info.get("serial_number", ""),
                }
                att_dict[xkey] = updates[xkey]

            # 次コマへまたがる場合 (next_period_data != None) は、次のスロットへ書き込み
            if next_period_data and new_course_idx < 4:
                next_e, next_x = next_period_data

                # 本来 entry{new_course_idx+1} に書くが、そこが埋まってればさらに先へずらす
                slot_for_next = ensure_slot_is_free(att_dict, updates, new_course_idx + 1)
                next_ekey = f"entry{slot_for_next}"
                next_xkey = f"exit{slot_for_next}"
                print(f"[DEBUG] -> 次コマデータを slot={slot_for_next} に書き込み (entry={next_e}, exit={next_x})")

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

            # 「ずらし」や「次コマ追加入力」で updates が溜まった場合 → Firebase に反映
            if updates:
                att_path = f"Students/attendance/student_id/{student_id}"
                update_data_in_firebase(att_path, updates)

            # コースごとの最終判定ステータスを Firebase に保存
            decision_path = f"Students/attendance/student_id/{student_id}/course_id/{cid_int}/decision"
            set_data_in_firebase(decision_path, status)
            results_dict[(student_index, new_course_idx, date_str, cid_int)] = status

    print("[DEBUG] === シート書き込み処理を開始します。===")
    # ----------------------------------------------------
    # ここから下はGoogleスプレッドシートへの書き込み例
    # ----------------------------------------------------
    all_student_index_data = student_info_data.get("student_index", {})
    for std_idx, info_val in all_student_index_data.items():
        sheet_id = info_val.get("sheet_id")
        if not sheet_id:
            print(f"[DEBUG] student_index={std_idx} に sheet_id がありません。スキップ。")
            continue

        # 指定のSpreadSheetを開く
        try:
            print(f"[DEBUG] Google SpreadSheetを開きます: sheet_id={sheet_id}")
            sh = gclient.open_by_key(sheet_id)
        except Exception as e:
            print(f"[DEBUG] シートを開けませんでした。例外: {e}")
            continue

        # 書き込み対象の出席結果
        std_result_items = {k: v for k, v in results_dict.items() if k[0] == std_idx}
        if not std_result_items:
            print(f"[DEBUG] student_index={std_idx} に該当する出席判定結果がありません。スキップします。")
            continue

        # enrollment情報からこの学生が履修しているcourse_id一覧を取得
        enroll_info = enrollment_data_all.get(std_idx)
        if not enroll_info or "course_id" not in enroll_info:
            continue
        enrolled_course_str = enroll_info["course_id"]
        enrolled_course_ids = [c.strip() for c in enrolled_course_str.split(",") if c.strip()]

        print(f"[DEBUG] -> student_index={std_idx} への書き込み対象: {std_result_items}")
        # (student_index, new_course_idx, date_str, cid_int) = status_val
        for (s_idx, new_course_idx, date_str, cid_int), status_val in std_result_items.items():
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # "dd"

            # yyyymmシートを開く or 無ければ新規作成
            try:
                ws = sh.worksheet(yyyymm)
                print(f"[DEBUG] 既存のワークシート {yyyymm} を取得しました。")
            except gspread.exceptions.WorksheetNotFound:
                print(f"[DEBUG] ワークシート {yyyymm} が見つからないため、新規作成します。")
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            cid_str = str(cid_int)
            try:
                # コースIDのインデックス (enrolled_course_idsの何番目か)
                course_pos = enrolled_course_ids.index(cid_str)
            except ValueError:
                print(f"[DEBUG] cid_str={cid_str} が enrolled_course_ids に見つからずスキップします。")
                continue

            # シート上の行列決め(例: 行=コース順+2, 列=日付+1)
            row = course_pos + 2
            col = day + 1

            print(f"[DEBUG] シート[{yyyymm}] (row={row}, col={col}) に '{status_val}' を書き込みます。")
            ws.update_cell(row, col, status_val)

    print("=== 出席判定処理＆シート書き込み完了 ===")


if __name__ == "__main__":
    process_attendance_and_write_sheet()
