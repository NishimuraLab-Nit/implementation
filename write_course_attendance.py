import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------
# Firebase & GSpread初期化
# ---------------------
if not firebase_admin._apps:
    # Firebaseの認証情報を読み込む
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })
    print("Firebaseが初期化されました。")

# GSpreadの認証設定
scope = ["https://spreadsheets.google.com/feeds", 
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
gclient = gspread.authorize(creds)
print("Google Sheets APIが認証されました。")

# ---------------------
# Firebaseアクセス関連
# ---------------------
def get_data_from_firebase(path):
    """
    指定されたFirebaseのパスからデータを取得します。
    """
    print(f"Firebaseパスからデータを取得中: {path}")
    ref = db.reference(path)
    data = ref.get()
    if data is None:
        print(f"パス {path} にデータが存在しません。")
    return data

# ---------------------
# Google Sheetsの日付行取得関連
# ---------------------
def get_or_create_date_row(sheet, current_date):
    """
    シート内で現在の日付に対応する行を取得します。
    存在しない場合は新しい行を追加します。
    日付はシートの1列目にあると仮定します。
    """
    print(f"シート内で日付 {current_date} の行を検索中...")
    try:
        cell = sheet.find(current_date)
        row = cell.row
        print(f"日付 {current_date} は既に行 {row} に存在します。")
    except gspread.exceptions.CellNotFound:
        # 日付が見つからない場合は新しい行を追加
        row = len(sheet.get_all_values()) + 1
        sheet.update_cell(row, 1, current_date)
        print(f"日付 {current_date} を新たに行 {row} に追加しました。")
    return row

# ---------------------
# Google Sheetsの学生列取得関連
# ---------------------
def get_student_columns(sheet, student_ids):
    """
    シート内で各学生IDに対応する列番号を取得します。
    存在しない場合は新しい列を追加します。
    学生IDはシートの1行目にあると仮定します。
    """
    print("シート内で学生IDに対応する列を検索中...")
    header = sheet.row_values(1)
    student_columns = {}
    for student_id in student_ids:
        try:
            cell = sheet.find(student_id)
            column = cell.col
            print(f"学生ID {student_id} は列 {column} に存在します。")
        except gspread.exceptions.CellNotFound:
            # 学生IDが見つからない場合は新しい列を追加
            column = len(header) + 1
            sheet.update_cell(1, column, student_id)
            print(f"学生ID {student_id} を新たに列 {column} に追加しました。")
        student_columns[student_id] = column
    return student_columns

# ---------------------
# メイン処理
# ---------------------
def main():
    # 現在の日付を取得（例: '2025-01-26'）
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')
    print(f"現在の日付: {current_date}")

    # Courses一覧を取得
    courses_path = "Courses"
    courses = get_data_from_firebase(courses_path)
    if not courses:
        print("コースが見つかりません。処理を終了します。")
        return

    course_ids = list(courses.keys())
    print(f"取得したコースID一覧: {course_ids}")

    # 一致するコース（特定の日付に基づく）をフィルタリング
    matching_course_ids = []
    for course_id in course_ids:
        schedule_day = get_data_from_firebase(f"Courses/{course_id}/schedule/day")
        print(f"コース {course_id} のスケジュール日付: {schedule_day}")
        if schedule_day == current_date:
            matching_course_ids.append(course_id)

    print(f"今日の日付に一致するコースID: {matching_course_ids}")

    # 一致するコースごとに処理
    for course_id in matching_course_ids:
        # コースに登録されている学生のインデックスを取得
        enrollment_path = f"Students/enrollment/course_id/{course_id}/student_index"
        student_indices_str = get_data_from_firebase(enrollment_path)
        if not student_indices_str:
            print(f"コース {course_id} に登録されている学生がいません。次のコースへ進みます。")
            continue
        # 学生インデックスをリストに変換（例: "a, b" -> ['a', 'b']）
        student_indices = [idx.strip() for idx in student_indices_str.split(',')]
        print(f"コース {course_id} の学生インデックス: {student_indices}")

        # Students/student_info/student_index/{studnet_idx}/student_idを取得
        student_ids = []
        for student_idx in student_indices:
            student_id = get_data_from_firebase(f"Students/student_info/student_index/{student_idx}/student_id")
            if not student_id:
                print(f"学生インデックス {student_idx} に対応するstudent_idが見つかりません。")
                continue
            student_ids.append(student_id)
        if not student_ids:
            print(f"コース {course_id} に有効な学生がいません。次のコースへ進みます。")
            continue

        # Courses/{course_id}/course_sheet_idからsheet_idを取得
        sheet_id = get_data_from_firebase(f"Courses/{course_id}/course_sheet_id")
        if not sheet_id:
            print(f"コース {course_id} のsheet_idが見つかりません。次のコースへ進みます。")
            continue

        # Google Sheetsを開く
        try:
            sheet = gclient.open_by_key(sheet_id).sheet1  # 最初のシートを使用
            print(f"シート {sheet_id} を開きました。")
        except Exception as e:
            print(f"シート {sheet_id} を開く際にエラーが発生しました: {e}")
            continue

        # 日付に対応する行を取得または作成
        row = get_or_create_date_row(sheet, current_date)

        # 学生IDに対応する列を取得または作成
        student_columns = get_student_columns(sheet, student_ids)

        # 各学生について処理
        for student_idx, student_id in zip(student_indices, student_ids):
            # Students/attendance/student_id/{student_id}/course_id/{course_id}/decisionを取得
            decision = get_data_from_firebase(f"Students/attendance/student_id/{student_id}/course_id/{course_id}/decision")
            if decision is None:
                print(f"学生 {student_id} のコース {course_id} に対するdecisionが見つかりません。次の学生へ進みます。")
                continue

            print(f"学生 {student_id} のdecision: {decision}")

            # Google Sheetsの指定セルにdecisionを入力
            try:
                column = student_columns.get(student_id)
                if not column:
                    print(f"学生 {student_id} の列番号が見つかりません。")
                    continue
                sheet.update_cell(row, column, decision)
                print(f"シート {sheet_id} のセル (行: {row}, 列: {column}) にdecisionを入力しました。")
            except Exception as e:
                print(f"シート {sheet_id} のセル更新時にエラーが発生しました: {e}")

if __name__ == "__main__":
    main()
