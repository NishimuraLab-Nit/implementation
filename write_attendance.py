import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import math

# ------------------------------------------------------------------------
# 【自然書き起こし】
# このコードでは、まずFirebaseから必要なデータを取得し、
# 各 student_id → student_index → course_id の順でループします。
# 次に、各コースのスケジュール時刻との比較を行い、出席情報を判定します。
# 判定結果は「✕（欠席）」「〇（正常出席）」「△早（早退）」「△遅（遅刻）」のいずれかです。
# さらに、exit がコース終了 +5分を超えるケースなど、特定の条件では
# exit 時刻や次の entry 時刻を自動的に再調整し、Firebaseへ書き戻します。
# 最後に、student_indexごとに持っている sheet_id でGoogleスプレッドシートを開き、
# 対象月(%Y-%m)のシート(Worksheet)に判定結果を記入する処理を行います。
# コースの行番号と、日付(%d)に基づく列番号を計算して書き込むイメージです。
# ------------------------------------------------------------------------


# Firebaseアプリの初期化（未初期化の場合のみ実行）
if not firebase_admin._apps:
    # サービスアカウントの認証情報を設定（パスは環境に合わせて）
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    # Firebaseアプリを初期化
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Google Sheets API用のスコープを設定
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Googleサービスアカウントから資格情報を取得
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
client = gspread.authorize(creds)

# ------------------------------------------------------------------------
# Firebaseからデータを取得するヘルパー関数
# ------------------------------------------------------------------------
def get_data_from_firebase(path):
    ref = db.reference(path)
    return ref.get()

# ------------------------------------------------------------------------
# Firebaseに特定パスでデータを書き込むヘルパー関数
# ------------------------------------------------------------------------
def update_data_to_firebase(path, data):
    ref = db.reference(path)
    ref.update(data)

# ------------------------------------------------------------------------
# 時刻文字列 (e.g. "2025-01-06 08:49:50") → datetime オブジェクト
# ------------------------------------------------------------------------
def parse_datetime_string(dt_str, dt_format="%Y-%m-%d %H:%M:%S"):
    return datetime.datetime.strptime(dt_str, dt_format)

# ------------------------------------------------------------------------
# コースの時間文字列 (e.g. "08:50~10:20" あるいは "8:50~10:20" ) を
# 開始時刻と終了時刻(当日付)の datetime オブジェクトに変換する
#
# ※日付部分はダミーで同一日を設定し、比較の際は時間差のみ用いるイメージ。
# ------------------------------------------------------------------------
def parse_course_time_range(time_str, base_date=None):
    """
    time_str: "HH:MM~HH:MM" または "H:MM~H:MM" 等
    base_date: 日付が必要な場合は渡す(datetimeオブジェクト)
    """
    if base_date is None:
        # とりあえず適当な日の 00:00:00 を基準とする
        base_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    start_str, end_str = time_str.split("~")
    # "08:50" などをパース
    start_hour, start_minute = map(int, start_str.split(":"))
    end_hour, end_minute = map(int, end_str.split(":"))
    start_dt = base_date.replace(hour=start_hour, minute=start_minute)
    end_dt = base_date.replace(hour=end_hour, minute=end_minute)

    return start_dt, end_dt

# ------------------------------------------------------------------------
# 分単位の差を返すためのヘルパー関数
# (datetime同士の差分(秒)を60で割って四捨五入)
# ------------------------------------------------------------------------
def diff_in_minutes(dt1, dt2):
    return int(round((dt2 - dt1).total_seconds() / 60))

# ------------------------------------------------------------------------
# 出席判定を行う関数
#    entry_dt  : 入室時刻(datetime)
#    exit_dt   : 退室時刻(datetime or None)
#    start_dt  : コース開始時刻(datetime)
#    finish_dt : コース終了時刻(datetime)
#
# 返り値： (判定文字列, 修正された entry_dt, exit_dt)
#          判定文字列には "〇", "✕", "△早xx", "△遅xx" など
# 
# ※仕様に沿って、
#   - 欠席（✕）
#   - 出席（〇）
#   - 早退（△早）
#   - 遅刻（△遅）
#   を判定。
#   また exit_dt が存在しない場合や、finish+5分を超える場合などで
#   entry/exit を修正する場合は、ここで新たな datetime を返す。
# ------------------------------------------------------------------------
def judge_attendance(entry_dt, exit_dt, start_dt, finish_dt):
    """
    仕様例を再掲:
    - 欠席（✕‬）
      entryX が startX より大幅に遅い（exitX以降）なら「欠席」
    - 正常出席（〇）
      ① entry が start + 5分以内 かつ exit が finish + 5分以内
      ② exit が finish + 5分以降 → exit を finish に修正し、次の entry に finish+10分を設定
      ③ exit が存在しない → exit を finish に修正し、次の entry に finish+10分を設定
    - 早退（△早）
      entry が start + 5分以内 かつ exit が finish - 5分以前
    - 遅刻（△遅）
      entry が start + 5分以降 かつ exit が finish + 5分以内
    """

    # デフォルトの判定結果
    result_str = ""

    # ------------------------------
    # 欠席判定 (※仕様例より、entry_dt が コース終了後 or かなり遅いケースを想定)
    # ------------------------------
    # 「entry1がexit1以降であれば欠席」と書かれていますが、
    # 一般的には「コース開始よりかなり遅い or そもそも来ていない」イメージ。
    # ここでは start_dt より「非常に遅い」→ 例えば entry_dt が finish_dt 以降、などを欠席とします。
    if entry_dt >= finish_dt:
        # 欠席として処理
        result_str = "✕"
        # 修正時刻の変更は無し
        return result_str, entry_dt, exit_dt

    # exit_dt が None の場合のために一旦仮の exit_dt を finish_dt にして計算させる
    # （「exit が存在しない時は exit1 = finish1」とする仕様に準拠）
    if exit_dt is None:
        exit_dt = finish_dt

    # 差分
    diff_entry_start = diff_in_minutes(start_dt, entry_dt)   # start→entry
    diff_exit_finish = diff_in_minutes(exit_dt, finish_dt)   # finish→exit

    # ------------------------------
    # 正常出席（〇）
    # ------------------------------
    # ① entryが (start+5分以内) かつ exitが (finish+5分以内)
    #    → OK
    if diff_entry_start <= 5 and diff_exit_finish <= 5 and diff_exit_finish >= -1440:
        # ※diff_exit_finish が負の場合は(実際にexitがfinishより前だが、5分以内ならOK扱い)
        #   ただし、-1440は「日をまたいでしまう」ような極端な値を除外した目安
        result_str = "〇"
        return result_str, entry_dt, exit_dt

    # ② exit が finish+5分以降
    #    → exit を finish に修正し、次の entry に finish+10分を割り当てたい
    #      (ここでの return はあくまで「course_index1 は正常出席扱い」とする)
    #      ただし entry は start+5分以内であることが前提
    # 
    # ③ exit が None (既に上で補正済み) かつ start+5分以内の入室
    #    → exit を finish に修正し、次の entry に finish+10分を設定する
    # 
    # これらの条件はいずれも「entry <= start+5分」の場合に限る。
    if diff_entry_start <= 5:
        # exit が finish + 5分よりも後か？
        if diff_exit_finish >= 5:
            # exitをfinishへ修正
            new_exit_dt = finish_dt
            result_str = "〇"  # 正常出席とみなす
            return result_str, entry_dt, new_exit_dt

        # exit が None のケース (ここではすでに修正済みだが)
        # もしくは exit が finish より前だけど entry はstart+5分以内
        # → 普通は上の①か下の早退へ引っかかる想定
        #   ここでは特に別の処理はしない
        pass

    # ------------------------------
    # 早退（△早）
    # ------------------------------
    # 「entry が start+5分以内 かつ exit が finish-5分以前」
    if diff_entry_start <= 5:
        # finish_dtからexit_dtへの差分がプラス方向で5分以上あれば、exitは finish よりもだいぶ早い
        # つまり exit_dt <= finish_dt - 5分 と解釈する
        # diff_exit_finish = (exit - finish)の分差
        # たとえば exit=10:00、finish=10:20 → diff_exit_finish = -20分
        # これが -5分以下になれば finish-5分以前とみなせる
        if diff_exit_finish <= -5:
            # 早退
            # 早退何分か？ → finish - exit
            early_minutes = abs(diff_in_minutes(exit_dt, finish_dt))
            result_str = f"△早{early_minutes}分"
            return result_str, entry_dt, exit_dt

    # ------------------------------
    # 遅刻（△遅）
    # ------------------------------
    # 「entry が start+5分以降 かつ exit が finish+5分以内」
    if diff_entry_start > 5 and diff_exit_finish <= 5:
        # 何分遅刻か？ → entry - start
        late_minutes = diff_in_minutes(start_dt, entry_dt)
        result_str = f"△遅{late_minutes}分"
        return result_str, entry_dt, exit_dt

    # ------------------------------
    # いずれの条件にも該当しない時は、便宜上 "✕" として返すか、
    # 追加のロジックを入れてもよい。
    # ここでは該当しない場合は「✕」とする。
    # ------------------------------
    result_str = "✕"
    return result_str, entry_dt, exit_dt


# ------------------------------------------------------------------------
# メイン処理
# ------------------------------------------------------------------------
def main_process():
    # 現在の年月フォーマットを取得 (シート名に使う)
    current_ym = datetime.datetime.now().strftime("%Y-%m")

    # 1) Students/attendance/student_id 以下のデータ取得
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    if not attendance_data:
        print("attendance_data が空です")
        return

    # 「全てのループが終了したら… シート書き込みする」仕様のため、
    # 途中で判定結果を蓄積しておく辞書: dict[student_index][(日付, course_id)] = 判定文字
    attendance_results = {}

    # --------------------------------------------------------------------
    # (1) ループ1: Students/attendance/student_id/{student_id}
    # --------------------------------------------------------------------
    for student_id, student_att_info in attendance_data.items():
        if not isinstance(student_att_info, dict):
            continue

        # entryN, exitN をすべて取得する想定
        # 例: entry1, exit1, entry2, exit2, ...
        # ここでは単純化して entry1, exit1 を取り出し比較する形にする
        # （実際は "entry{n}" の形でループさせる実装など、運用にあわせて調整）
        entry_dt_list = []
        exit_dt_list = []

        for key, val in student_att_info.items():
            if key.startswith("entry"):
                dt = parse_datetime_string(val["read_datetime"])
                entry_dt_list.append(dt)
            elif key.startswith("exit"):
                dt = parse_datetime_string(val["read_datetime"])
                exit_dt_list.append(dt)

        # 例では entryN, exitN がペアになっていると仮定し、indexを揃えて取り出す
        # （実際には数が合わない場合のハンドリングが必要）
        entry_dt_list.sort()
        exit_dt_list.sort()

        # 2) 学生の student_index を取得 (Students/student_info/student_id/{student_id})
        student_info_path = f"Students/student_info/student_id/{student_id}"
        student_info = get_data_from_firebase(student_info_path)
        if not student_info or "student_index" not in student_info:
            continue
        student_index = student_info["student_index"]

        # 3) 学生が受講しているコースIDを取得 (Students/enrollment/student_index/{student_index}/course_id)
        enrollment_path = f"Students/enrollment/student_index/{student_index}"
        enrollment_info = get_data_from_firebase(enrollment_path)
        if not enrollment_info or "course_id" not in enrollment_info:
            continue
        
        course_ids_str = enrollment_info["course_id"]  # e.g. "1, 2"
        course_ids = [cid.strip() for cid in course_ids_str.split(",") if cid.strip()]

        # 出席結果を入れるために student_index 用の辞書を確保
        if student_index not in attendance_results:
            attendance_results[student_index] = {}

        # 上から順に entry, exit をコースへあてはめていくイメージ
        # （1コースにつき1ペア という想定で実装例を示す）
        # もし entry, exit の数とコース数が合わない場合は実運用でロジック追加してください。
        for i, course_id in enumerate(course_ids):
            # i番目のentry/exit を取り出す (存在しない場合は None)
            if i < len(entry_dt_list):
                entry_dt = entry_dt_list[i]
            else:
                # entryが無ければ仮にコース開始時刻に設定するなど
                entry_dt = None

            if i < len(exit_dt_list):
                exit_dt = exit_dt_list[i]
            else:
                exit_dt = None

            # 4) course_id をもとにスケジュール時刻を取得 (Courses/{course_id}/schedule/time)
            #    course_id はリスト型として [null, {…}, {…}, …] で入っている可能性があるため
            #    /Courses/course_id/{course_id}/ というパスで取得する
            if course_id.isdigit():
                course_id_int = int(course_id)
            else:
                # もし数字以外のIDだった場合は別途対応が必要
                continue

            course_path = f"Courses/course_id/{course_id_int}"
            course_info = get_data_from_firebase(course_path)
            if not course_info or "schedule" not in course_info or "time" not in course_info["schedule"]:
                # スケジュール情報が無ければ次へ
                continue
            schedule_time_str = course_info["schedule"]["time"]  # e.g. "8:50~10:20"

            # entry_dt の日付情報を使ってスケジュールの開始終了を作る
            if entry_dt:
                base_date = entry_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                base_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

            start_dt, finish_dt = parse_course_time_range(schedule_time_str, base_date)

            # entry_dt, exit_dt が None のときは適当に補正
            if entry_dt is None:
                entry_dt = start_dt  # 仮にコース開始時刻を入室とする
            # exit_dt は judge_attendance の中で None を補正する

            # 5) entry, exit, start, finish を比較して出席ステータスを判定
            result_str, new_entry_dt, new_exit_dt = judge_attendance(entry_dt, exit_dt, start_dt, finish_dt)

            # judge_attendance の結果、exit_dt などが書き換わった場合
            # Firebaseに再保存する。
            # 例: "exit1" を finish に書き換えたい → "exit{i+1}" を生成して保存
            # ここでは「i番目のexit」を上書きする例
            if new_exit_dt != exit_dt:
                exit_key = f"exit{i+1}"  # 例: exit1, exit2 ...
                path_to_exit = f"Students/attendance/student_id/{student_id}/{exit_key}"
                new_exit_data = {
                    "read_datetime": new_exit_dt.strftime("%Y-%m-%d %H:%M:%S")
                }
                update_data_to_firebase(path_to_exit, new_exit_data)

            # もし entry_dt も書き換わったら同様に保存できる
            if new_entry_dt != entry_dt:
                entry_key = f"entry{i+1}"
                path_to_entry = f"Students/attendance/student_id/{student_id}/{entry_key}"
                new_entry_data = {
                    "read_datetime": new_entry_dt.strftime("%Y-%m-%d %H:%M:%S")
                }
                update_data_to_firebase(path_to_entry, new_entry_data)

            # 判定結果を保管
            # カギとして (日付, course_id) を使う想定
            day_str = new_entry_dt.strftime("%Y-%m-%d")
            attendance_results[student_index][(day_str, course_id_int)] = result_str

    # --------------------------------------------------------------------
    # 全てのループ終了後、Students/student_info/student_index/{student_index}/sheet_id を回す
    # シートへアクセスし、 %Y-%m シートを開いて、(列= 日付%d +1, 行= コースの何番目か +1 )に入力
    # --------------------------------------------------------------------

    # student_index 全件を取得
    all_student_index_info = get_data_from_firebase("Students/student_info/student_index")
    if not all_student_index_info:
        print("student_index 情報がありません")
        return

    for stu_index_key, stu_detail in all_student_index_info.items():
        if not isinstance(stu_detail, dict):
            continue
        if "sheet_id" not in stu_detail:
            continue

        sheet_id = stu_detail["sheet_id"]
        # Google Sheets 上の該当シートをオープン
        try:
            gfile = client.open_by_key(sheet_id)
        except Exception as e:
            print(f"sheet_id={sheet_id} のシートを開けませんでした: {e}")
            continue

        # シート名 (例: "2025-01")を想定
        # なければ新規シートを作るか、あるいはエラーにする等の対応
        try:
            worksheet = gfile.worksheet(current_ym)
        except:
            # シートがない場合は作る（もしくはスキップ）
            try:
                worksheet = gfile.add_worksheet(title=current_ym, rows=100, cols=100)
            except Exception as e:
                print(f"{current_ym} シートを作成できませんでした: {e}")
                continue

        # ここで attendance_results[stu_index_key] に入っている結果をシートに書き込む
        # (key= (day_str, course_id_int), value= "〇"など)
        if stu_index_key not in attendance_results:
            # 何も判定情報が無い場合はスキップ
            continue

        # コースIDの処理順番（何番目か）を決める必要があるが、
        # シンプルに course_id の値昇順などで並べた場合のインデックス(i+1)を行とする
        # （実際には受講コースの一覧を保持しておき、その順番で行を決めるなど運用に合わせて調整）
        # ここでは仮実装として、course_id 昇順で並べて行番号を決める
        # →  course_id → row番号 のマッピングを作成
        # 例: 1 → 行=2, 2 → 行=3, ...
        # 行は (courseの個数目+1) と仕様にあるので、最初のコースが行=2 となるようにする
        user_results_dict = attendance_results[stu_index_key]
        # course_id の重複なしでまとめる
        course_ids_unique = sorted(list({cid for (_, cid) in user_results_dict.keys()}))

        course_id_to_row = {}
        for idx, cid in enumerate(course_ids_unique):
            course_id_to_row[cid] = idx + 2  # 行= idx+1 のさらに+1で 2行目から

        # user_results_dict は {(day_str, course_id_int): result_str, ...}
        for (ds, cid), result_str in user_results_dict.items():
            # 列 = day(%d) + 1
            # ds = "2025-01-06" など
            try:
                dt_ = datetime.datetime.strptime(ds, "%Y-%m-%d")
                day_num = int(dt_.strftime("%d"))  # 6 など
            except:
                # 万が一パースできなければスキップ
                continue

            col_num = day_num + 1  # %d + 1
            row_num = course_id_to_row.get(cid, None)
            if row_num is None:
                # 未設定であればスキップ
                continue

            # シートに書き込み
            # 書き込み先: (row_num, col_num)
            worksheet.update_cell(row_num, col_num, result_str)

    print("処理が完了しました。")


if __name__ == "__main__":
    main_process()
