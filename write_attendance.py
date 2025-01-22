import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------
# Firebase & GSpread初期化
# ---------------------
if not firebase_admin._apps:
    # サービスアカウントの認証情報を設定
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    # Firebaseアプリを初期化
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Google Sheets API用のスコープを設定
scope = ["https://spreadsheets.google.com/feeds", 
         "https://www.googleapis.com/auth/drive"]
# Googleサービスアカウントから資格情報を取得
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gclient = gspread.authorize(creds)

# ---------------------
# Firebaseからデータを取得する関数
# ---------------------
def get_data_from_firebase(path):
    """指定されたパスのデータをFirebase Realtime Databaseから取得する"""
    ref = db.reference(path)
    return ref.get()

def update_data_in_firebase(path, data_dict):
    """指定されたパスに対して、data_dictの内容で更新を行う"""
    ref = db.reference(path)
    ref.update(data_dict)

# ---------------------
# 日時関連のユーティリティ関数
# ---------------------
def parse_datetime(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    """文字列 dt_str を datetimeオブジェクトに変換する。失敗したらNone"""
    try:
        return datetime.datetime.strptime(dt_str, fmt)
    except Exception:
        return None

def parse_hhmm_range(range_str):
    """
    "HH:MM~HH:MM" 形式の文字列を開始時刻, 終了時刻(ともにdatetime.time)に変換して返す。
    """
    try:
        start_str, end_str = range_str.split("~")
        # "8:50" → (8, 50) のようにパース
        start_h, start_m = map(int, start_str.split(":"))
        end_h,   end_m   = map(int, end_str.split(":"))
        return datetime.time(start_h, start_m, 0), datetime.time(end_h, end_m, 0)
    except Exception:
        return None, None

def combine_date_and_time(date_dt, time_obj):
    """
    date部分(date_dt) と time部分(time_obj) を合わせて 新たな datetime を返す
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
# 出席判定ロジック
# ---------------------
def judge_attendance_for_course(entry_dt, exit_dt, start_dt, finish_dt):
    """
    1コマ(1コース) 分の出欠判定を行い、結果ステータスと必要に応じて
    entry_dt, exit_dt の更新情報を返す。
    
    仕様に合わせた判定:
    # 欠席（×）
      entry がそのコマ終了以降なら => 欠席
    # 正常出席（○）
      ① entry <= start +5分 かつ exit <= finish +5分
      ② exit > finish +5分 の場合 => exitをfinishに、次コマのentryを finish+10分 とする（保存用データ返却）
      ③ exitが存在しない場合 => exitをfinishにして保存
    # 早退 (△早)
      entry <= start+5分 かつ exit < finish-5分
    # 遅刻 (△遅)
      entry > start+5分 かつ exit <= finish+5分

    戻り値: (status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt)
      - status_str: "×", "○", "△早xx分", "△遅xx分" のいずれか
      - updated_entry_dt, updated_exit_dt: Firebaseに保存すべき(修正後)の entry/exit 
      - next_course_entry_dt: 次コマ用に自動生成された entry (もしあれば)
    """
    # デフォルト
    status_str = ""
    updated_entry_dt = entry_dt
    updated_exit_dt = exit_dt
    next_course_entry_dt = None

    # (1) 開始・終了5分の余裕
    td_5min  = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    # まずは欠席判定
    if entry_dt >= finish_dt:
        # このコマが終わった後に入室 → 欠席
        return "×", updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # 早退(△早)判定
    #   entry <= start+5分 かつ exit < finish-5分
    #   ただし、exit < finish-5分 "以前" なので「<=」でなく厳密に "<" とする場合は適宜変えてください
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt < (finish_dt - td_5min)):
        # 早退
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)  # 分差
        status_str = f"△早{delta_min}分"
        return status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # 遅刻(△遅)判定
    #   entry > start+5分 かつ exit <= finish+5分
    if (entry_dt > (start_dt + td_5min)) and (exit_dt <= (finish_dt + td_5min)):
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        status_str = f"△遅{delta_min}分"
        return status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # 正常出席(○)のパターン
    #   ① entry <= start+5分 かつ exit <= finish+5分
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt <= (finish_dt + td_5min)):
        status_str = "○"
        return status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt

    #   ② exitが finish+5分 以降
    if exit_dt > (finish_dt + td_5min):
        status_str = "○"  # とりあえずこのコマは正常扱い
        # exitをfinishにして、次コースのentryを finish+10分 にする
        original_exit = exit_dt  # 保管用
        updated_exit_dt = finish_dt

        # 次コマentryを finish+10分 にする
        next_course_entry_dt = finish_dt + td_10min
        # exitを次コマに引き継ぐ (exit2=original_exit)
        # → 次のコースで利用するため、呼び出し側に返す
        return status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt

    #   ③ exitが存在しない場合(=None) → finishで埋めて正常出席
    if exit_dt is None:
        status_str = "○"
        updated_exit_dt = finish_dt
        return status_str, updated_entry_dt, updated_exit_dt, next_course_entry_dt

    # もし上記以外のパターンがあれば適宜追加
    # デフォルト何もしなかったら "？" 等を返す
    return "？", updated_entry_dt, updated_exit_dt, next_course_entry_dt

# ---------------------
# メイン処理
# ---------------------
def process_attendance_and_write_sheet():
    """
    1) Students/attendance/student_id/{student_id} を取得して student_id 毎にループ
    2) Students/student_info/student_id/{student_id}/student_index を取得
    3) enrollment から受講コースリストを取得 (1,2,...) 
    4) entry/exit とコースのscheduleを比較 → 出欠判定
    5) 各student_indexの sheet_id を取得して GSpreadに接続し、判定結果を反映
       シート名: "YYYY-mm"
       セル: (row = コースの何個目か +1, col = 日付(%d) +1)
    """
    # 1) attendance の全student_idを取得
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("attendance データがありません。処理を終了します。")
        return

    # コース情報をまとめて取得 (course_id配列)
    courses_data = get_data_from_firebase("Courses/course_id")  # 0番目はNone
    # 各 student_index の sheet_id 取得用
    student_info_data = get_data_from_firebase("Students/student_info")

    # ループで結果を一旦メモリ上に保存しておき、最後にシート更新
    # results_dict[(student_index, course_id, date_str)] = 出席ステータス
    results_dict = {}

    for student_id, attendance_dict in attendance_data.items():
        if not isinstance(attendance_dict, dict):
            continue

        # 2) student_indexの取得
        student_index = None
        if (student_info_data.get("student_id") and 
            student_id in student_info_data["student_id"] and
            "student_index" in student_info_data["student_id"][student_id]):
            student_index = student_info_data["student_id"][student_id]["student_index"]
        if not student_index:
            # student_index取得できなければスキップ
            continue

        # 3) enrollment からコースID取得
        enroll_path = f"Students/enrollment/student_index/{student_index}"
        enrollment_data = get_data_from_firebase(enroll_path)
        if not enrollment_data or "course_id" not in enrollment_data:
            print(f"{student_index} の enrollment 情報がありません。")
            continue
        
        course_id_str = enrollment_data["course_id"]  # "1, 2" など
        course_id_list = [c.strip() for c in course_id_str.split(",") if c.strip()] if course_id_str else []
        if not course_id_list:
            print(f"{student_index} にコースIDの登録がありません。")
            continue

        # attendanceデータ内に複数entry/exitがある可能性を考慮して一旦ソートしておく
        # 例: entry1, exit1, entry2, exit2, ...
        # キー(例えば "entry1", "exit1", "entry2", "exit2", ... )でソート
        sorted_keys = sorted(attendance_dict.keys())

        # entry/exitをペアで管理するためのリスト作成
        # [('entry1', 'exit1'), ('entry2','exit2'), ...]
        # 実際にはexitNが存在しない場合もあるので考慮する
        entry_exit_pairs = []
        i = 1
        while True:
            ekey = f"entry{i}"
            xkey = f"exit{i}"
            if ekey not in attendance_dict:
                break
            entry_exit_pairs.append((ekey, xkey))
            i += 1

        # コースの個数分ループ( course_id_list )
        # ただし コース数に比べて entry/exitペアが足りない場合があるので注意
        # 指定仕様によれば、 course1 -> entry1/exit1, course2 -> entry2/exit2 のように進む想定

        pair_index = 0  # entry_exit_pairs のインデックス
        for idx, c_id_str in enumerate(course_id_list, start=1):
            # c_id_str は '1' '2' など文字列
            try:
                c_id = int(c_id_str)
            except:
                print(f"course_idのパースに失敗: {c_id_str}")
                continue
            if c_id <= 0 or c_id >= len(courses_data):
                print(f"courses_dataに存在しないc_idです: {c_id}")
                continue

            course_info = courses_data[c_id]
            # スケジュール情報を取得
            schedule_info = course_info.get("schedule", {})
            time_range_str = schedule_info.get("time")  # "8:50~10:20"等
            if not time_range_str:
                # スケジュールが無ければスキップ
                continue

            # まだ entry_exit ペアが残っていない → 欠席扱いとする(または空白扱い)
            if pair_index >= len(entry_exit_pairs):
                # 欠席とする
                # 書き込みのために結果保存
                # 日付が無いが、仮に当日の日付を使うか、あるいは「attendance_dictの最初のentry日付」など
                # ここでは「欠席(×)」のみ記録しておきます
                today_str = datetime.datetime.now().strftime("%Y-%m-%d")
                results_dict[(student_index, c_id, today_str)] = "×"
                continue

            # entry/exit のキーを取り出す
            ekey, xkey = entry_exit_pairs[pair_index]
            pair_index += 1  # 次コースへ行くときにインクリメント
            entry_rec = attendance_dict.get(ekey, {})
            exit_rec  = attendance_dict.get(xkey, {})

            entry_dt_str = entry_rec.get("read_datetime")
            exit_dt_str  = exit_rec.get("read_datetime")

            # datetimeにパース
            entry_dt = parse_datetime(entry_dt_str) if entry_dt_str else None
            exit_dt  = parse_datetime(exit_dt_str)  if exit_dt_str else None
            if not entry_dt:
                # entry無いなら ここでは欠席とする
                # (あるいは別途判定)
                today_str = datetime.datetime.now().strftime("%Y-%m-%d")
                results_dict[(student_index, c_id, today_str)] = "×"
                continue

            # コースの開始/終了時刻を当日のentry_dtの日付に合わせてdatetime化
            start_t, finish_t = parse_hhmm_range(time_range_str)
            if not start_t or not finish_t:
                continue

            # scheduleの日付(曜日)をどう扱うかは運用次第だが、
            # ここではentry_dtの年月日でコース開始・終了を作る
            start_dt  = combine_date_and_time(entry_dt, start_t)
            finish_dt = combine_date_and_time(entry_dt, finish_t)

            # 比較してステータスを得る
            status, new_entry_dt, new_exit_dt, next_course_entry_dt = judge_attendance_for_course(
                entry_dt, exit_dt, start_dt, finish_dt
            )
            # 戻ってきた new_entry_dt, new_exit_dt で Firebase 更新する必要があれば行う
            # entry, exitのread_datetimeを再構築
            # exit, entryのどちらかに変更があった(=new_xxx != old_xxx)場合のみ更新するイメージ
            updates = {}
            # entry更新チェック
            if new_entry_dt and new_entry_dt != entry_dt:
                updates[ekey] = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": entry_rec.get("serial_number", "")
                }
            # exit更新チェック
            if new_exit_dt and new_exit_dt != exit_dt:
                updates[xkey] = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": exit_rec.get("serial_number", "")
                }
            # 次コース用entry( entry2 )作成がある場合 => 次のペアが既に存在するか確認 or 新規に作る
            if next_course_entry_dt:
                # 次のentryキーは entry{pair_index+1} になる可能性が高いが、
                # 厳密には「もう1つ分のペアがあるかどうか」を先に見てもよい
                next_ekey = f"entry{pair_index+1}"
                # exitN は一旦 "exit{pair_index+1}" と想定
                next_xkey = f"exit{pair_index+1}"
                # exitN にはold exitを割り当てたいなら judge関数内で保持したoriginal_exitを使う、など
                # ここでは簡単に exit_dt(あるいは exit_rec )の中身をそのまま入れるサンプルにする
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

            # 出欠結果を日付単位でまとめる
            # シートに書き込むときは「シート名 = YYYY-mm」「列 = 日(%d) +1」「行 = コースの(何個目か)+1」
            date_str = entry_dt.strftime("%Y-%m-%d")
            results_dict[(student_index, c_id, date_str)] = status

    # -----------------
    # 5) シートに書き込み
    #    Students/student_info/student_index/{student_index}/sheet_id
    # -----------------
    all_student_index_data = student_info_data.get("student_index", {})
    # results_dictのキー: (student_index, c_id, date_str)
    # これを student_index 単位→日付単位→コースID単位 に分割して書き込み

    # student_indexごとにループ
    for std_idx, info_val in all_student_index_data.items():
        sheet_id = info_val.get("sheet_id")
        if not sheet_id:
            # sheet_idが無ければスキップ
            continue

        # まずシートにアクセス
        try:
            sh = gclient.open_by_key(sheet_id)
        except Exception as e:
            print(f"Spreadsheet({sheet_id})を開けません: {e}")
            continue

        # results_dict のうち, 当該student_index に関する結果のみ取り出し
        std_result_items = {k: v for k, v in results_dict.items() if k[0] == std_idx}
        if not std_result_items:
            # 特に書くことがなければスキップ
            continue

        # date_str ごとにまとめる
        # シートへの書き込みは "YYYY-mm" で Worksheetを取得し、 
        # cell(column = 日+1, row = コースの何個目+1) にstatusをセット
        # ここで「コースの何個目」は c_id を単純に row = c_id+1 としてしまうか、
        # あるいは student_index が保有するコースリストの順番通りに割り振るかは仕様次第。
        # ここではシンプルに c_id+1 を row とする例にする。

        for (s_idx, c_id, date_str), status_val in std_result_items.items():
            # シート名
            yyyymm = date_str[:7]  # "YYYY-MM"
            day = int(date_str[8:10])  # %d

            # Worksheetを得る(なければ新規作成)
            try:
                worksheet = sh.worksheet(yyyymm)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sh.add_worksheet(title=yyyymm, rows=50, cols=50)

            row = c_id + 1  # コースIDが1から始まる想定
            col = day + 1   # 日にち +1
            try:
                worksheet.update_cell(row, col, status_val)
            except Exception as e:
                print(f"シート書き込み失敗 [{sheet_id}:{yyyymm}({row},{col})]: {e}")

    print("=== 出席判定処理が完了しました ===")


if __name__ == "__main__":
    process_attendance_and_write_sheet()
