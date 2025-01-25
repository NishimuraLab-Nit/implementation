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
    """ dict形式のデータを update() でまとめて更新 """
    ref = db.reference(path)
    ref.update(data_dict)

def set_data_in_firebase(path, value):
    """ 単一の値(文字列など)を set() で更新 """
    ref = db.reference(path)
    ref.set(value)

# ---------------------
# ユーティリティ
# ---------------------
def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    """ 文字列をdatetimeにパース。失敗すれば None を返す """
    if not dt_str:
        return None
    try:
        return datetime.datetime.strptime(dt_str, fmt)
    except:
        return None

def combine_date_and_time(date_dt, time_obj):
    """ date部分と time部分を合体して datetime を作る """
    return datetime.datetime(
        date_dt.year, date_dt.month, date_dt.day,
        time_obj.hour, time_obj.minute, time_obj.second
    )

# period に対する開始～終了時刻のマッピング
PERIOD_TIME_MAP = {
    1: ("08:50", "10:20"),
    2: ("10:30", "12:00"),
    3: ("13:10", "14:40"),
    4: ("14:50", "16:20"),
}

def parse_hhmm(hhmm_str):
    """ 'HH:MM' を datetime.time にするユーティリティ """
    hh, mm = map(int, hhmm_str.split(":"))
    return datetime.time(hh, mm, 0)

# ---------------------
# 出席判定ロジック（新仕様）
# ---------------------
def judge_attendance_for_period(entry_dt, exit_dt, start_dt, finish_dt):
    """
    仕様に基づいた判定を行い、以下を返す:
      status_str, new_entry_dt, new_exit_dt, next_period_tuple
    next_period_tuple には (次コマのentry_dt, 次コマのexit_dt) を入れる場合がある
    """
    # タイムデルタ
    td_5min  = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    # 欠席（×）
    # 「entry_dt >= finish_dt」であれば "×"
    if entry_dt and entry_dt >= finish_dt:
        # もう entry が 授業終了時刻以降 → 欠席
        return "×", entry_dt, exit_dt, None

    # 早退（△早）
    # entry_dt <= start_dt+5分 かつ exit_dt < finish_dt-5分
    #  => "△早xx分" (xx = finish_dt - exit_dt の分数)
    if (entry_dt and exit_dt and 
        entry_dt <= start_dt + td_5min and 
        exit_dt < finish_dt - td_5min):
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        return f"△早{delta_min}分", entry_dt, exit_dt, None

    # 出席（〇） (大きく3パターン)
    # (1) entry_dt <= start_dt+5分 かつ exit_dt <= finish_dt+5分
    if (entry_dt and exit_dt and
        entry_dt <= start_dt + td_5min and 
        exit_dt <= finish_dt + td_5min):
        return "〇", entry_dt, exit_dt, None

    # (2) entry_dt <= start_dt+5分 かつ exit_dt > finish_dt+5分
    #     => exitをfinish_dtに書き換え、次コマ用に (finish_dt+10分, 元exit) を作成
    if (entry_dt and exit_dt and 
        entry_dt <= start_dt + td_5min and
        exit_dt > finish_dt + td_5min):
        status_str = "〇"
        original_exit = exit_dt
        updated_exit_dt = finish_dt
        next_entry_dt = finish_dt + td_10min
        next_exit_dt  = original_exit
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # (3) exit_dt が存在しない => exit_dtを finish_dt にして出席扱い
    if entry_dt and (exit_dt is None):
        status_str = "〇"
        updated_exit_dt = finish_dt
        # 次コマ用
        next_entry_dt = finish_dt + td_10min
        next_exit_dt = None
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # 遅刻（△遅）
    # entry_dt > start_dt+5分 かつ exit_dt <= finish_dt+5分
    # => "△遅xx分" (xx = entry_dt - start_dt の分数)
    if (entry_dt and exit_dt and
        entry_dt > start_dt + td_5min and 
        exit_dt <= finish_dt + td_5min):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分", entry_dt, exit_dt, None

    # その他のケース（判定不能）は "？"
    return "？", entry_dt, exit_dt, None


# ---------------------
# メインフロー
# ---------------------
def process_attendance_and_write_sheet():
    # コード実行時の曜日を取得（英語表記: Monday, Tuesday, ...）
    now = datetime.datetime.now()
    current_weekday_str = now.strftime("%A")

    # 各種データを取得
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("attendance データがありません。終了します。")
        return

    # Courses 全体
    courses_all = get_data_from_firebase("Courses/course_id")
    # 学生情報( student_info )
    student_info_data = get_data_from_firebase("Students/student_info")
    # enrollment データ
    enrollment_data_all = get_data_from_firebase("Students/enrollment/student_index")

    if not courses_all or not student_info_data or not enrollment_data_all:
        print("必要なデータが不足しています。終了します。")
        return

    # 出席判定結果をシートにまとめるための一時的な辞書
    # {(student_index, new_course_idx, date_str): status}
    results_dict = {}

    # -----------------
    # 学生ごとのループ
    # -----------------
    for student_id, att_dict in attendance_data.items():
        if not isinstance(att_dict, dict):
            continue

        # student_index の取得
        si_map = student_info_data.get("student_id", {})
        if student_id not in si_map:
            continue
        student_index = si_map[student_id].get("student_index")
        if not student_index:
            continue

        # enrollment (student_index -> { "course_id": "1,2,3" ... })
        enroll_info = enrollment_data_all.get(student_index)
        if not enroll_info or "course_id" not in enroll_info:
            continue

        # 学生が履修しているコース一覧（文字列 "1, 2, 3, ..." ）
        enrolled_course_str = enroll_info["course_id"]
        enrolled_course_ids = [
            c.strip() for c in enrolled_course_str.split(",") if c.strip()
        ]

        # -----------------
        # 当日の曜日に合致するコースだけを抽出
        # (Courses/{course_id}/schedule/day == current_weekday_str)
        # -----------------
        valid_course_list = []
        for cid_str in enrolled_course_ids:
            try:
                cid_int = int(cid_str)
            except:
                continue
            # 配列風に格納されている場合: courses_all[cid_int]
            if cid_int < 0 or cid_int >= len(courses_all):
                continue
            course_info = courses_all[cid_int]
            if not course_info:
                continue

            sched = course_info.get("schedule", {})
            day_in_course = sched.get("day", "")      # "Monday" など
            period_in_course = sched.get("period", 0) # 数字(1～4)
            if day_in_course == current_weekday_str:
                valid_course_list.append(cid_int)

        # new_course_index の保存
        # (仕様通り: 「dayが曜日と一致したcourse_idだけを取得し new_course_index=[course_id] として保存」)
        new_course_index_path = f"Students/attendance/student_id/{student_id}/new_course_index"
        set_data_in_firebase(new_course_index_path, ",".join(map(str, valid_course_list)))

        # entry/exit は periodごとに entry{period}, exit{period} が想定されている
        # 全日付が混在しうるが、ここでは最初のエントリの日付を基準日とする
        # （本来は「当日分だけ抽出」などの工夫が必要だが、ここでは既存コードを踏襲）
        # --------------------------------------------------------
        # まず最初に存在する entryX の日付を基準日とする
        base_date = None
        for i in range(1, 5):
            ekey = f"entry{i}"
            if ekey in att_dict:
                dt_tmp = parse_datetime(att_dict[ekey].get("read_datetime", ""))
                if dt_tmp:
                    base_date = dt_tmp.date()
                    break

        if not base_date:
            # そもそもエントリ時刻が無ければスキップ
            continue

        date_str = base_date.strftime("%Y-%m-%d")

        # -----------------
        # valid_course_list に含まれるコースごとに判定
        # -----------------
        # new_course_idx はシート書き込み時に行を決めるために使っていたので
        # enumerate で1から振る形は踏襲
        # （元コードの意図に沿うため）
        # -----------------
        for new_course_idx, cid_int in enumerate(valid_course_list, start=1):
            course_info = courses_all[cid_int]
            sched = course_info.get("schedule", {})
            period_in_course = sched.get("period", 0)

            # period=1～4 以外はスキップ
            if not (1 <= period_in_course <= 4):
                continue

            # 対象の entry/exit を取得
            ekey = f"entry{period_in_course}"
            xkey = f"exit{period_in_course}"
            entry_dt = None
            exit_dt = None

            # もし entry(該当period) が無い場合は欠席扱いする
            if ekey not in att_dict:
                # 欠席
                status = "×"
                # Firebase保存
                decision_path = f"Students/attendance/student_id/{student_id}/course_id/{cid_int}/decision/{date_str}"
                set_data_in_firebase(decision_path, f"decision={status}")
                # シート書き込み用
                results_dict[(student_index, new_course_idx, date_str)] = status
                continue

            # entry_dt, exit_dt をパース
            entry_info = att_dict.get(ekey, {})
            exit_info  = att_dict.get(xkey, {})
            entry_dt = parse_datetime(entry_info.get("read_datetime", ""))
            exit_dt  = parse_datetime(exit_info.get("read_datetime", ""))

            # periodから start_dt, finish_dt を算出
            start_hhmm_str, finish_hhmm_str = PERIOD_TIME_MAP[period_in_course]
            start_t = parse_hhmm(start_hhmm_str)
            finish_t = parse_hhmm(finish_hhmm_str)
            start_dt = combine_date_and_time(base_date, start_t)
            finish_dt = combine_date_and_time(base_date, finish_t)

            # 出席判定
            status, new_entry_dt, new_exit_dt, next_period_data = judge_attendance_for_period(
                entry_dt, exit_dt, start_dt, finish_dt
            )

            # 変更がある場合は Firebase の entry/exit を更新
            updates = {}
            if new_entry_dt and (new_entry_dt != entry_dt):
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_info.get("serial_number", "")
                }
                att_dict[ekey] = updates[ekey]
            if new_exit_dt and exit_dt and (new_exit_dt != exit_dt):
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_info.get("serial_number", "")
                }
                att_dict[xkey] = updates[xkey]
            elif (new_exit_dt and exit_dt is None):
                # exit_dtがNone だった場合に新規追加するケース
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_info.get("serial_number", "")
                }
                att_dict[xkey] = updates[xkey]

            # 次コマ用 entry/exit を作成する場合 (exit > finish + 5minなどのケース)
            if next_period_data:
                next_e, next_x = next_period_data
                # 次 period=period_in_course+1 の entry/exitキーを仮に作成
                # ただし、もし既に entry/exit があるなら上書きするかどうかは設計次第
                # ここでは元コードの方針に合わせ「entry{n+1}, exit{n+1}」を追加
                next_ekey = f"entry{period_in_course + 1}"
                next_xkey = f"exit{period_in_course + 1}"

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

            # Firebase 更新
            if updates:
                att_path = f"Students/attendance/student_id/{student_id}"
                update_data_in_firebase(att_path, updates)

            # 出席判定結果を Firebase に保存
            decision_path = f"Students/attendance/student_id/{student_id}/course_id/{cid_int}/decision"
            set_data_in_firebase(decision_path, f"{status}")

            # シート書き込み用に一時保存
            results_dict[(student_index, new_course_idx, date_str)] = status

    # -----------------
    # シート書き込み
    # -----------------
    # 元コードをほぼ踏襲し、(student_index, new_course_idx, date_str) でループ
    all_student_index_data = student_info_data.get("student_index", {})
    for std_idx, info_val in all_student_index_data.items():
        sheet_id = info_val.get("sheet_id")
        if not sheet_id:
            continue
        try:
            sh = gclient.open_by_key(sheet_id)
        except:
            # 該当のシートが開けない場合はスキップ
            continue

        # 当該 student_index に対応する結果をフィルタ
        std_result_items = {
            k: v for k, v in results_dict.items() if k[0] == std_idx
        }
        if not std_result_items:
            continue

        for (s_idx, new_course_idx, date_str), status_val in std_result_items.items():
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # "dd" (01~31)
            # 該当年月のワークシートを取得 / 無ければ作成
            try:
                ws = sh.worksheet(yyyymm)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            # （元コード同様）
            #   行 = new_course_idx + 1
            #   列 = 日付 + 1
            row = new_course_idx + 1
            col = day + 1
            ws.update_cell(row, col, status_val)

    print("=== 出席判定処理＆シート書き込み完了 ===")


if __name__ == "__main__":
    process_attendance_and_write_sheet()
