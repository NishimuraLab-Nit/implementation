import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ---------------------
# Firebase & GSpread初期化 (省略: ここは前回までと同じ)
# ---------------------
if not firebase_admin._apps:
    cred = credentials.Certificate("/tmp/firebase_service_account.json")
    firebase_admin.initialize_app(cred, {"databaseURL": "https://test-xxxxx-default-rtdb.firebaseio.com/"})
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/gcp_service_account.json", scope)
gclient = gspread.authorize(creds)


def get_data_from_firebase(path):
    ref = db.reference(path)
    return ref.get()


def update_data_in_firebase(path, data_dict):
    ref = db.reference(path)
    ref.update(data_dict)


def set_data_in_firebase(path, value):
    ref = db.reference(path)
    ref.set(value)


def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    if not dt_str:
        return None
    try:
        return datetime.datetime.strptime(dt_str, fmt)
    except:
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


PERIOD_TIME_MAP = {
    1: ("08:50", "10:20"),
    2: ("10:30", "12:00"),
    3: ("13:10", "14:40"),
    4: ("14:50", "16:20"),
}


def judge_attendance_for_period(entry_dt, exit_dt, start_dt, finish_dt):
    td_5min = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

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

    # 出席(〇)パターン1: 時間内に入退室が収まる
    if (
        entry_dt
        and exit_dt
        and entry_dt <= (start_dt + td_5min)
        and exit_dt <= (finish_dt + td_5min)
    ):
        return "〇", entry_dt, exit_dt, None

    # 出席(〇)パターン2: 次コマにまたがる (exit_dt >= finish_dt+5分 など)
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
        # 本来ここでは「△早～」かもしれないケースもあるが、細かいロジックは割愛
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

    # 上記以外は「？」扱い
    return "？", entry_dt, exit_dt, None


def ensure_slot_is_free(att_dict, slot_idx):
    """
    slot_idx番のentry/exitが既に使われている場合、
    そのデータを空いている後ろのスロットへ順番にずらしていく。
    
    返り値: 実際に書き込みに使える "最終的なスロット番号"
    """
    # 1日最大4コマ想定で、slot_idx～4 の範囲でチェック
    for i in range(slot_idx, 5):  # 5は含まない(range(1,5)→1,2,3,4)
        ekey = f"entry{i}"
        xkey = f"exit{i}"

        # このスロットが「entryもexitも無い」なら、ここは空いているので OK
        if (ekey not in att_dict) and (xkey not in att_dict):
            return i

        # もしここが埋まっているなら、さらに一つ先へ「ずらす」必要がある
        # ただし i=4 だった場合は、これ以上先がないので上書きしかできない
        if i == 4:
            # 4番スロットが埋まっていたら仕方なく上書き、という形になる
            return 4

        # i < 4 の場合は、i+1 へずらす
        ekey_next = f"entry{i+1}"
        xkey_next = f"exit{i+1}"

        # もし i+1 がさらに埋まっている場合は、再帰的にさらに先へずらす
        # 先に i+1 スロットを確保してから、このスロットを移動する
        slot_for_next = ensure_slot_is_free(att_dict, i+1)

        # slot_for_next が返ってきたら、そこへ移す
        ekey_shift = f"entry{slot_for_next}"
        xkey_shift = f"exit{slot_for_next}"

        # 今の i番スロットのデータを slot_for_next のスロットに移動
        if ekey in att_dict:
            att_dict[ekey_shift] = att_dict[ekey]
            del att_dict[ekey]
        if xkey in att_dict:
            att_dict[xkey_shift] = att_dict[xkey]
            del att_dict[xkey]

        # i番スロットは空になったはず
        return i  # i番が空いたので i番を返して終了

    # ここは通常到達しない
    return slot_idx


def process_attendance_and_write_sheet():
    now = datetime.datetime.now()
    current_weekday_str = now.strftime("%A")

    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        return

    courses_all = get_data_from_firebase("Courses/course_id")
    student_info_data = get_data_from_firebase("Students/student_info")
    enrollment_data_all = get_data_from_firebase("Students/enrollment/student_index")
    if not courses_all or not student_info_data or not enrollment_data_all:
        return

    results_dict = {}

    for student_id, att_dict in attendance_data.items():
        if not isinstance(att_dict, dict):
            continue

        # student_index
        si_map = student_info_data.get("student_id", {})
        if student_id not in si_map:
            continue
        student_index = si_map[student_id].get("student_index")
        if not student_index:
            continue

        # enrollment
        enroll_info = enrollment_data_all.get(student_index)
        if not enroll_info or "course_id" not in enroll_info:
            continue
        enrolled_course_str = enroll_info["course_id"]
        enrolled_course_ids = [c.strip() for c in enrolled_course_str.split(",") if c.strip()]

        # 今日の曜日に合致するcourse_idだけ抽出
        valid_course_list = []
        for cid_str in enrolled_course_ids:
            try:
                cid_int = int(cid_str)
            except:
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

        # 基準日 (entry1～4 のいずれか)
        base_date = None
        for i in range(1, 5):
            ekey_test = f"entry{i}"
            dt_tmp = parse_datetime(att_dict.get(ekey_test, {}).get("read_datetime", ""))
            if dt_tmp:
                base_date = dt_tmp.date()
                break
        if not base_date:
            continue

        date_str = base_date.strftime("%Y-%m-%d")

        # valid_course_listを順に処理(1番目→entry1, 2番目→entry2, ...)
        for new_course_idx, (schedule_period, cid_int) in enumerate(valid_course_list, start=1):
            if not (1 <= schedule_period <= 4):
                continue

            ekey = f"entry{new_course_idx}"
            xkey = f"exit{new_course_idx}"

            # entry{n} が無ければ欠席扱い
            if ekey not in att_dict:
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

            status, new_entry_dt, new_exit_dt, next_period_data = judge_attendance_for_period(
                entry_dt, exit_dt, start_dt, finish_dt
            )

            updates = {}
            if new_entry_dt and (new_entry_dt != entry_dt):
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_info.get("serial_number", ""),
                }
                att_dict[ekey] = updates[ekey]

            if new_exit_dt and (new_exit_dt != exit_dt):
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_info.get("serial_number", ""),
                }
                att_dict[xkey] = updates[xkey]
            elif new_exit_dt and not exit_dt:
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_info.get("serial_number", ""),
                }
                att_dict[xkey] = updates[xkey]

            # ★★★「次コマへまたがる」場合、次スロットを確保して書き込む ★★★
            if next_period_data and new_course_idx < 4:
                next_e, next_x = next_period_data
                # まず "entry{new_course_idx+1}" を使いたいが、被っていたらずらす
                target_slot = ensure_slot_is_free(att_dict, new_course_idx + 1)
                next_ekey = f"entry{target_slot}"
                next_xkey = f"exit{target_slot}"

                if next_e:
                    updates[next_ekey] = {
                        "read_datetime": next_e.strftime("%Y-%m-%d %H:%M:%S"),
                        "serial_number": entry_info.get("serial_number", ""),  # 仮に同じカードで入室とみなす
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

    # ---- (以下、シートへの書き込み処理は省略／前回と同様でOK) ----
    # 省略...
    print("=== 出席判定＆更新が完了しました ===")


if __name__ == "__main__":
    process_attendance_and_write_sheet()
