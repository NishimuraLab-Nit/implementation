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

# ---------------------
# Firebaseアクセス関連
# ---------------------
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

# ---------------------
# ユーティリティ
# ---------------------
def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    """文字列をdatetimeにパース。失敗すれば None を返す"""
    if not dt_str:
        return None
    try:
        dt = datetime.datetime.strptime(dt_str, fmt)
        return dt
    except Exception as e:
        print(f"[DEBUG] parse_datetime: 変換失敗 ({dt_str}) {e}")
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
def judge_attendance_for_period(entry_dt, exit_dt, start_dt, finish_dt, period_in_course):
    """
    【仕様変更要点】
      - period=1 のときのみ、新しい仕様(②, ③など)を適用
      - period=2,3,4 は従来通りのロジックを適用

    仕様②:
      ・entry1 が「start1＋5分以内」かつ exit1 が「finish1+5分以降」の場合
        ⇒ exit1をfinish1に上書きし、
           新たに entry2=finish1+10分, exit2=元のexit1 を作成（Firebaseに保存）,
           period=1は「〇」を記録
      ・entry1 が start1+5分以降 かつ exit1 が finish1+5分以降 ⇒ 「△早{delta_min}分」
      ・entry1 が start1+5分以内 かつ exit1 が finish1+5分以前 ⇒ 「△遅{delta_min}分」

    仕様③:
      ・もし exit1 が存在しない場合 ⇒ exit1=finish1, entry2=finish1+10分 を作成,
        period=1は「〇」を記録

    戻り値:
      status_str, new_entry_dt, new_exit_dt, (next_entry_dt, next_exit_dt)
       ※次コマの entry/exit を作成する場合はタプルで返す
    """
    import datetime
    td_5min  = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    # entry_dt が finish_dt 以降なら欠席扱い
    if (entry_dt is None) or (entry_dt >= finish_dt):
        return "×", entry_dt, exit_dt, None

    # -----------------------------------
    # period=1 の場合: 新仕様を適用
    # -----------------------------------
    if period_in_course == 1:
        # ③ exit1 が存在しない場合
        if exit_dt is None:
            # exit1 = finish1
            updated_exit_dt = finish_dt
            # 次コマの entry2=finish1+10分, exit2=None
            next_entry_dt = finish_dt + td_10min
            next_exit_dt  = None
            # period=1 は「〇」
            return "〇", entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

        # ②-1: entry1 ≤ start1+5分 かつ exit1 ≥ finish1+5分 ⇒ 「〇」
        #       exit1=finish1 に上書き, entry2=finish1+10分, exit2=元exit1
        if (entry_dt <= start_dt + td_5min) and (exit_dt >= finish_dt + td_5min):
            original_exit = exit_dt
            updated_exit_dt = finish_dt  # exit1 → finish1
            next_entry_dt = finish_dt + td_10min  # entry2
            next_exit_dt  = original_exit        # exit2
            return "〇", entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

        # ②-2: entry1 >= start1+5分 かつ exit1 >= finish1+5分 ⇒ 「△早{delta_min}分」
        #       ※delta_min は一例として (exit_dt - finish_dt)
        if (entry_dt >= start_dt + td_5min) and (exit_dt >= finish_dt + td_5min):
            delta_min = int((exit_dt - finish_dt).total_seconds() // 60)
            return f"△早{delta_min}分", entry_dt, exit_dt, None

        # ②-3: entry1 ≤ start1+5分 かつ exit1 ≤ finish1+5分 ⇒ 「△遅{delta_min}分」
        #       ※delta_min は一例として (start_dt - entry_dt)
        if (entry_dt <= start_dt + td_5min) and (exit_dt <= finish_dt + td_5min):
            delta_min = int((start_dt - entry_dt).total_seconds() // 60)
            return f"△遅{delta_min}分", entry_dt, exit_dt, None

        # 上記に該当しない場合はデフォルト「〇」
        return "〇", entry_dt, exit_dt, None

    # -----------------------------------
    # period=2,3,4 の場合: 従来ロジック
    # -----------------------------------
    else:
        # (1) 欠席: 上で return しているのでここでは不要

        # (2) 早退 (△早)
        #    entry <= start+5分 AND exit < finish-5分
        td_5min = datetime.timedelta(minutes=5)
        if (entry_dt <= (start_dt + td_5min)) and (exit_dt < (finish_dt - td_5min)):
            delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
            return f"△早{delta_min}分", entry_dt, exit_dt, None

        # (3-1) 通常の〇
        if (entry_dt <= (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt <= (finish_dt + td_5min)):
            return "〇", entry_dt, exit_dt, None

        # (3-2) exit >= finish+5分
        #       exit1=finish, 次コマ entry2=finish+10分, exit2=元exit
        if (entry_dt <= (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt >= (finish_dt + td_5min)):
            original_exit = exit_dt
            updated_exit_dt = finish_dt
            next_entry_dt = finish_dt + td_10min
            next_exit_dt  = original_exit
            return "〇", entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

        # (3-3) exit_dt が None → exit=finish, entry=finish+10
        if exit_dt is None:
            updated_exit_dt = finish_dt
            next_entry_dt   = finish_dt + td_10min
            return "〇", entry_dt, updated_exit_dt, (next_entry_dt, None)

        # (4) 遅刻 (△遅)
        #    entry > start+5分 AND exit <= finish+5分
        if (entry_dt > (start_dt + td_5min)) and (exit_dt <= (finish_dt + td_5min)):
            delta_min = int((entry_dt - start_dt).total_seconds() // 60)
            return f"△遅{delta_min}分", entry_dt, exit_dt, None

        # (5) その他
        return "？", entry_dt, exit_dt, None


# ---------------------
# メインフロー
# ---------------------
def process_attendance_and_write_sheet():
    # 現在の曜日（例: Monday, Tuesday, ...）
    now = datetime.datetime.now()
    current_weekday_str = now.strftime("%A")
    print(f"[DEBUG] 現在の曜日: {current_weekday_str}")

    # Firebaseから当日の出席データ等を取得
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

    # シート書き込み用に (student_index, new_course_idx, date_str) -> status を一時保存
    results_dict = {}

    print("[DEBUG] === 学生ごとのループを開始します。===")
    for student_id, att_dict in attendance_data.items():
        if not isinstance(att_dict, dict):
            print(f"[DEBUG] student_id={student_id} の attendance_data が辞書ではありません。スキップします。")
            continue

        # student_index を取得
        si_map = student_info_data.get("student_id", {})
        if student_id not in si_map:
            print(f"[DEBUG] student_id={student_id} が student_info_data に見つからず。スキップします。")
            continue
        student_index = si_map[student_id].get("student_index")
        if not student_index:
            print(f"[DEBUG] student_id={student_id} の student_index が空。スキップします。")
            continue

        # enrollment からコースIDの文字列を取得, カンマ区切りをリスト化
        enroll_info = enrollment_data_all.get(student_index)
        if not enroll_info or "course_id" not in enroll_info:
            print(f"[DEBUG] enrollment_info が存在しないか、course_id がありません。スキップします。")
            continue
        enrolled_course_str = enroll_info["course_id"]
        enrolled_course_ids = [
            c.strip() for c in enrolled_course_str.split(",") if c.strip()
        ]

        # 当日(曜日)に合うコースだけを抽出
        valid_course_list = []
        for cid_str in enrolled_course_ids:
            try:
                cid_int = int(cid_str)
            except:
                continue
            if cid_int < 0 or cid_int >= len(courses_all):
                continue
            course_info = courses_all[cid_int]
            sched = course_info.get("schedule", {})
            day_in_course = sched.get("day", "")
            if day_in_course == current_weekday_str:
                valid_course_list.append(cid_int)

        # entry1, entry2, entry3, entry4 のうち、最初に存在するものの日付を基準日とする
        base_date = None
        for i in range(1, 5):
            ekey = f"entry{i}"
            if ekey in att_dict:
                dt_tmp = parse_datetime(att_dict[ekey].get("read_datetime", ""))
                if dt_tmp:
                    base_date = dt_tmp.date()
                    break

        if not base_date:
            print(f"[DEBUG] student_id={student_id} に entry1～4 が無いためスキップします。")
            continue

        date_str = base_date.strftime("%Y-%m-%d")
        print(f"[DEBUG] => student_id={student_id} / 基準日: {date_str}")

        # 有効なコースIDリストをループ
        for new_course_idx, cid_int in enumerate(valid_course_list, start=1):
            course_info = courses_all[cid_int]
            sched = course_info.get("schedule", {})
            period_in_course = sched.get("period", 0)
            if not (1 <= period_in_course <= 4):
                continue

            ekey = f"entry{period_in_course}"
            xkey = f"exit{period_in_course}"

            # entryが無い場合は欠席(×)
            if ekey not in att_dict:
                status = "×"
                decision_path = f"Students/attendance/student_id/{student_id}/course_id/{cid_int}/decision"
                set_data_in_firebase(decision_path, status)
                results_dict[(student_index, new_course_idx, date_str)] = status
                continue

            entry_info = att_dict.get(ekey, {})
            exit_info  = att_dict.get(xkey, {})
            entry_dt = parse_datetime(entry_info.get("read_datetime", ""))
            exit_dt  = parse_datetime(exit_info.get("read_datetime", ""))

            # period→開始終了時刻を生成
            start_hhmm_str, finish_hhmm_str = PERIOD_TIME_MAP[period_in_course]
            start_t  = parse_hhmm(start_hhmm_str)
            finish_t = parse_hhmm(finish_hhmm_str)
            start_dt  = combine_date_and_time(base_date, start_t)
            finish_dt = combine_date_and_time(base_date, finish_t)

            # 出席判定（新仕様込み）
            status, new_entry_dt, new_exit_dt, next_period_data = judge_attendance_for_period(
                entry_dt, exit_dt, start_dt, finish_dt, period_in_course
            )
            print(f"[DEBUG] => 判定結果: {status}")

            # Firebase更新用の辞書
            updates = {}

            # entry_dt が更新された場合
            if new_entry_dt and (new_entry_dt != entry_dt):
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_info.get("serial_number", "")
                }
                att_dict[ekey] = updates[ekey]

            # exit_dt が更新された場合
            if new_exit_dt and (exit_dt is not None) and (new_exit_dt != exit_dt):
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_info.get("serial_number", "")
                }
                att_dict[xkey] = updates[xkey]
            elif new_exit_dt and (exit_dt is None):
                # exit_dt が None だった場合、新たに作成
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_info.get("serial_number", "")
                }
                att_dict[xkey] = updates[xkey]

            # 次コマ用データがあれば作成
            if next_period_data:
                next_e, next_x = next_period_data
                np = period_in_course + 1
                if 1 <= np <= 4:
                    next_ekey = f"entry{np}"
                    next_xkey = f"exit{np}"

                    # entryX の作成
                    if next_e is not None:
                        updates[next_ekey] = {
                            "read_datetime": next_e.strftime("%Y-%m-%d %H:%M:%S"),
                            "serial_number": entry_info.get("serial_number", "")
                        }
                        att_dict[next_ekey] = updates[next_ekey]

                    # exitX の作成
                    if next_x is not None:
                        updates[next_xkey] = {
                            "read_datetime": next_x.strftime("%Y-%m-%d %H:%M:%S"),
                            "serial_number": exit_info.get("serial_number", "")
                        }
                        att_dict[next_xkey] = updates[next_xkey]

            # 変更があれば Firebase 更新
            if updates:
                att_path = f"Students/attendance/student_id/{student_id}"
                update_data_in_firebase(att_path, updates)

            # 「決定結果」を Firebase に保存
            decision_path = f"Students/attendance/student_id/{student_id}/course_id/{cid_int}/decision"
            set_data_in_firebase(decision_path, status)

            # シート書き込み用に記録
            results_dict[(student_index, new_course_idx, date_str)] = status

    # ---------------------
    # シート書き込み
    # ---------------------
    print("[DEBUG] === シート書き込み処理を開始します。===")
    all_student_index_data = student_info_data.get("student_index", {})
    for std_idx, info_val in all_student_index_data.items():
        sheet_id = info_val.get("sheet_id")
        if not sheet_id:
            continue
        try:
            sh = gclient.open_by_key(sheet_id)
        except Exception as e:
            print(f"[DEBUG] シートを開けませんでした。例外: {e}")
            continue

        # 同じ student_index の結果を取り出す
        std_result_items = {
            k: v for k, v in results_dict.items() if k[0] == std_idx
        }
        if not std_result_items:
            continue

        for (s_idx, new_course_idx, date_str), status_val in std_result_items.items():
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # "dd"
            try:
                ws = sh.worksheet(yyyymm)
            except gspread.exceptions.WorksheetNotFound:
                # 指定年月のワークシートが無ければ新規作成
                ws = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            # row, col は例として (row=コース順, col=日付) にマッピング
            row = new_course_idx + 1
            col = day + 1
            ws.update_cell(row, col, status_val)

    print("=== 出席判定処理＆シート書き込み完了 ===")

if __name__ == "__main__":
    process_attendance_and_write_sheet()
