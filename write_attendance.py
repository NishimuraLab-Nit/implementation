import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ===============================
# Firebase・GSpread初期化
# ===============================
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gspread_client = gspread.authorize(creds)

# ===============================
# Firebaseヘルパー関数
# ===============================
def get_data_from_firebase(path):
    """Firebase Realtime Database の path からデータを取得して返す"""
    ref = db.reference(path)
    return ref.get()

def update_data_in_firebase(path, data):
    """Firebase Realtime Database の path にデータを保存(更新)する"""
    ref = db.reference(path)
    ref.update(data)

# ===============================
# 時刻・日付パース用ヘルパー
# ===============================
def parse_datetime(dt_str):
    """
    例: "2025-01-06 08:49:50" -> datetime.datetime(2025, 1, 6, 8, 49, 50)
    """
    return datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")

def parse_time_range(time_str):
    """
    例: "8:50~10:20" -> (8, 50, 10, 20)
    """
    start_str, end_str = time_str.split("~")
    start_h, start_m = map(int, start_str.split(":"))
    end_h, end_m = map(int, end_str.split(":"))
    return start_h, start_m, end_h, end_m

def combine_date_time(base_date, hour, minute):
    """
    base_date: datetime.date or datetime.datetime
    時刻(hour, minute)を合わせた新しいdatetimeを返す
    """
    return datetime.datetime(base_date.year, base_date.month, base_date.day, hour, minute)

# ===============================
# 出欠判定ロジック
# ===============================
def judge_attendance(entry_dt, exit_dt, start_dt, finish_dt):
    """
    entry_dt: 入室時刻(datetime)
    exit_dt: 退室時刻(datetime) / 無い場合は None
    start_dt: コース開始時刻(datetime)
    finish_dt:コース終了時刻(datetime)

    戻り値:
      (attend_status, fix_entry_next, fix_exit_cur, fix_exit_next, note)
        - attend_status : "〇", "✕", "△早", "△遅" など
        - fix_entry_next: 次コースのentryを強制的に書き換える場合の時刻 (Noneなら修正不要)
        - fix_exit_cur  : このコースのexitを修正する場合の時刻 (Noneなら修正不要)
        - fix_exit_next : 次コースのexitを強制的に作成する場合の時刻 (Noneなら不要)
        - note          : 備考("△早10分"など)
    """
    import datetime
    delta_5min = datetime.timedelta(minutes=5)
    delta_10min = datetime.timedelta(minutes=10)

    # entry_dt が無い場合は欠席扱い
    if entry_dt is None:
        return ("✕", None, None, None, "")

    # -----------------------
    # ② exit_dt >= finish_dt + 5分
    # -----------------------
    # 「一旦 exit1 を old_exit に保存、exit1=finish1, entry2=finish1+10分, exit2=old_exit」
    # コース1は正常出席"〇"扱い
    if (exit_dt is not None) and (exit_dt >= finish_dt + delta_5min) and (entry_dt <= start_dt + delta_5min):
        old_exit = exit_dt
        fix_exit_cur = finish_dt            # exit1 を強制的に授業終了時刻へ
        fix_entry_next = finish_dt + delta_10min  # 次コースの entry (entry2)
        fix_exit_next = old_exit           # 次コースの exit (exit2) = 元の退室時刻

        return ("〇", fix_entry_next, fix_exit_cur, fix_exit_next, "")

    # -----------------------
    # ③ exit_dt が存在しない
    # -----------------------
    # 「exit1=finish1, entry2=finish1+10分」
    # コース1は正常出席"〇"扱い
    if exit_dt is None and (entry_dt <= start_dt + delta_5min):
        fix_exit_cur = finish_dt
        fix_entry_next = finish_dt + delta_10min
        fix_exit_next = None  # 次コースの exit は特に上書きしない

        return ("〇", fix_entry_next, fix_exit_cur, fix_exit_next, "")

    # -----------------------
    # それ以外の判定例
    # -----------------------
    # 早退 (△早)、遅刻 (△遅) などの既存ロジックは以下に続く
    # ※一例で記載しています。実際の条件はご要望どおりに調整してください。
    if (entry_dt <= start_dt + delta_5min) and \
       (exit_dt is not None) and (exit_dt <= finish_dt - delta_5min):
        # 早退(△早)
        diff_min = int((finish_dt - exit_dt).total_seconds() // 60)
        note_str = f"△早{diff_min}分"
        return ("△早", None, None, None, note_str)

    if (entry_dt > start_dt + delta_5min) and \
       (exit_dt is not None) and (exit_dt <= finish_dt + delta_5min):
        # 遅刻(△遅)
        diff_min = int((entry_dt - start_dt).total_seconds() // 60)
        note_str = f"△遅{diff_min}分"
        return ("△遅", None, None, None, note_str)

    # 正常出席(〇) の基本パターン
    if (entry_dt <= start_dt + delta_5min) and \
       (exit_dt is not None) and (exit_dt <= finish_dt + delta_5min):
        return ("〇", None, None, None, "")

    # どの条件にも当てはまらない場合 => 欠席(✕) とみなす
    return ("✕", None, None, None, "")

# ===============================
# メイン処理
# ===============================
def main_process():
    # -----------
    # 1) attendance/student_id 以下をループ (学生ごとの出席情報を取得)
    # -----------
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("No attendance data.")
        return

    # コース情報を一括取得(配列形式)
    courses_data = get_data_from_firebase("Courses/course_id")
    # 例: courses_data[1], courses_data[2], ...

    # 出席結果を一時保存する辞書
    # {(student_index, date_str, course_id) : "〇"/"✕"/"△早"/"△遅" + note}
    attendance_result_dict = {}

    # 後でFirebaseに強制修正を保存するためのデータを保持する
    # [(修正対象のentry_path, 修正値), (修正対象のexit_path, 修正値), ...]
    firebase_updates = []

    for student_id, entries_exits_dict in attendance_data.items():
        if not entries_exits_dict:
            continue
        
        # -----------
        # 2) student_info の student_id/{student_id} から student_index を取得
        # -----------
        path_info = f"Students/student_info/student_id/{student_id}"
        student_info = get_data_from_firebase(path_info)
        if not student_info or "student_index" not in student_info:
            # student_indexが見つからなければスキップ
            continue
        
        student_index = student_info["student_index"]

        # -----------
        # 3) enrollment の student_index/{student_index}/course_id を取得
        #    カンマ区切りの複数コースIDをリスト化
        # -----------
        path_enroll = f"Students/enrollment/student_index/{student_index}"
        enroll_info = get_data_from_firebase(path_enroll)
        if not enroll_info or "course_id" not in enroll_info:
            continue
        
        course_id_str = enroll_info["course_id"]  # 例: "1, 2"
        course_ids = [cid.strip() for cid in course_id_str.split(",") if cid.strip()]

        # attendance配下に entry1, exit1, entry2, exit2... のように複数あるケースが想定される
        # それらを番号順にペアで処理する
        # 例: "entry1", "exit1", "entry2", "exit2", ...
        # 順番を揃えるため keys() をソートしておく
        sorted_keys = sorted(entries_exits_dict.keys())  # ["entry1", "exit1", "entry2", ...]

        # entry/exitのペアをまとめる
        entry_exit_pairs = []
        current_entry = None
        current_exit = None
        current_index = None  # "1","2"など数字部

        for k in sorted_keys:
            v = entries_exits_dict[k]
            # 例 v["read_datetime"] = "2025-01-06 08:49:50"
            if not isinstance(v, dict):
                continue
            dt_str = v.get("read_datetime")
            if not dt_str:
                continue
            
            # entry, exit の番号を判定する
            # k例: "entry1" or "exit1" => "entry" or "exit" + "1"
            if k.startswith("entry"):
                current_index = k.replace("entry", "")
                current_entry = parse_datetime(dt_str)
            elif k.startswith("exit"):
                current_index = k.replace("exit", "")
                current_exit = parse_datetime(dt_str)

            # entry, exit 両方が揃ったタイミングでペア完成
            # 次のペアへ
            if current_entry and current_exit:
                entry_exit_pairs.append((current_index, current_entry, current_exit))
                current_index = None
                current_entry = None
                current_exit = None

        # entry と exit の数が合わずに最後が entry だけ残っているケース
        if current_entry and not current_exit:
            entry_exit_pairs.append((current_index, current_entry, None))

        # -----------
        # コース単位でループ (loop3)
        # -----------
        # course_ids = ["1", "2", ...]
        # 受講コース数に応じて entry_exit_pairs を順番に割り当てる想定
        pair_idx = 0  # entry_exit_pairs のインデックス
        for i, course_id in enumerate(course_ids, start=1):
            # course_idが整数の範囲か・courses_dataに存在するかチェック
            # (courses_dataは1始まりを想定しているため)
            try:
                int_course_id = int(course_id)
            except ValueError:
                # 数字でなければスキップ
                continue

            if int_course_id >= len(courses_data) or courses_data[int_course_id] is None:
                # コースデータが存在しない
                continue
            
            course_info = courses_data[int_course_id]
            schedule_info = course_info.get("schedule", {})
            time_str = schedule_info.get("time")  # 例: "8:50~10:20"
            if not time_str:
                # スケジュール情報がないコースはスキップ
                continue

            # entry_exit_pairsがもうなければ欠席扱い
            if pair_idx >= len(entry_exit_pairs):
                # 欠席
                # コース開始日の情報がないため、日付そのものが取れないケースがある
                # 今回は結果だけ入れる
                # (実運用では「対象日」が分かるように別途管理する)
                date_str = datetime.datetime.now().strftime("%Y-%m-%d")  # 例として現在日
                attendance_result_dict[(student_index, date_str, int_course_id)] = "✕"
                continue

            # ペアを取得
            idx_str, entry_dt, exit_dt = entry_exit_pairs[pair_idx]

            # 日付(date)は entry_dt から抜き取る (同日の授業想定)
            date_str = entry_dt.strftime("%Y-%m-%d")

            # コーススケジュールの開始・終了時刻(datetime)作成
            start_h, start_m, end_h, end_m = parse_time_range(time_str)
            # entry_dtの日付に紐づけた開始・終了時刻
            start_dt = combine_date_time(entry_dt, start_h, start_m)
            finish_dt = combine_date_time(entry_dt, end_h, end_m)

            # 出欠判定
            attend_status, fix_entry_next, fix_exit_cur, note = judge_attendance(
                entry_dt, exit_dt, start_dt, finish_dt
            )

            # 出欠マーク + note をまとめてセルに入れたい場合はここで結合
            if note:
                mark = f"{attend_status}({note})"
            else:
                mark = attend_status

            # 結果を dict に保存
            attendance_result_dict[(student_index, date_str, int_course_id)] = mark

            # Firebase強制修正がある場合
            if fix_exit_cur is not None:
                # exitX の更新
                exit_key_path = f"Students/attendance/student_id/{student_id}/exit{idx_str}"
                # 退室時刻を再設定
                fix_data = {
                    "read_datetime": fix_exit_cur.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": schedule_info.get("serial_number", "")
                }
                firebase_updates.append((exit_key_path, fix_data))

            if fix_entry_next is not None:
                # 次のコースのentryXを更新
                # 次コースは idx+1 になるため、entry_key_path も "entry{次番号}" などに変える
                # ただし exit_idx と同じ or +1 にするなど運用次第
                next_idx = str(int(idx_str) + 1)  # 次のインデックス文字
                entry_key_path = f"Students/attendance/student_id/{student_id}/entry{next_idx}"
                fix_data = {
                    "read_datetime": fix_entry_next.strftime("%Y-%m-%d %H:%M:%S"),
                    "serial_number": schedule_info.get("serial_number", "")
                }
                firebase_updates.append((entry_key_path, fix_data))

            # コース1つ消化したらペアを次に進める
            pair_idx += 1

    # -------------------
    # Firebaseの更新まとめて実行
    # -------------------
    for path, val in firebase_updates:
        update_data_in_firebase(path, val)

    # -----------
    # ループ終了後:
    # student_info > student_index > {student_index} > sheet_id を全取得し
    # 各シートへアクセスして結果を書き込み
    # -----------
    student_info_index_data = get_data_from_firebase("Students/student_info/student_index")
    if not student_info_index_data:
        print("No student_info index data.")
        return

    # 「シート名 = %Y-%m」は、日付ごとに月シートを切り替える場合を想定
    # attendance_result_dict のキー (student_index, date_str, course_id) から
    # 月ごとにシートをまとめて書き込む
    # 例: date_str = "2025-01-06" => シート名 "2025-01"
    sheets_cache = {}  # {(sheet_id, "YYYY-MM"): worksheetオブジェクト} キャッシュ
    for (st_idx, date_str, c_id), result_mark in attendance_result_dict.items():
        # student_index ごとに sheet_id を取得
        st_info = student_info_index_data.get(st_idx)
        if not st_info or "sheet_id" not in st_info:
            continue
        target_sheet_id = st_info["sheet_id"]

        # シート名作成 (YYYY-MM)
        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        sheet_name = date_obj.strftime("%Y-%m")

        # ワークシート取得(キャッシュ活用)
        if (target_sheet_id, sheet_name) not in sheets_cache:
            try:
                sh = gspread_client.open_by_key(target_sheet_id)
                try:
                    ws = sh.worksheet(sheet_name)
                except gspread.exceptions.WorksheetNotFound:
                    # なければ新規作成 or スキップ
                    ws = sh.add_worksheet(title=sheet_name, rows="100", cols="100")
                sheets_cache[(target_sheet_id, sheet_name)] = ws
            except Exception as e:
                print(f"Error opening sheet {target_sheet_id}: {e}")
                continue

        worksheet = sheets_cache[(target_sheet_id, sheet_name)]

        # カラム = 日付(%d) + 1
        col_idx = date_obj.day + 1
        # 行 = コースの個数目 + 1 (c_id が1始まりなら そのまま+1 で)
        # ただし c_id が 大きい数字の場合はその行に書く想定
        row_idx = c_id + 1

        # セル書き込み
        worksheet.update_cell(row_idx, col_idx, result_mark)

    print("Done.")

# メイン処理呼び出し
if __name__ == "__main__":
    main_process()
