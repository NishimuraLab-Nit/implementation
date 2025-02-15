import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ---------------------
# Firebase & GSpread初期化
# ---------------------
if not firebase_admin._apps:
    cred = credentials.Certificate("/tmp/firebase_service_account.json")
    firebase_admin.initialize_app(
        cred,
        {
            "databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/",
        },
    )
    print(f"[Debug] Firebase initialized.")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/gcp_service_account.json", scope)
gclient = gspread.authorize(creds)
print(f"[Debug] Google Sheets API authorized.")


# ---------------------
# Firebaseアクセス関連
# ---------------------
def get_data_from_firebase(path):
    print(f"[Debug] Fetching data from Firebase path: {path}")
    ref = db.reference(path)
    data = ref.get()
    if data is None:
        print(f"[Debug] No data found at path: {path}")
    return data


# ---------------------
# ヘルパー関数
# ---------------------
def get_current_date_details():
    now = datetime.datetime.now()
    current_day = now.strftime("%A")           # 例: "Sunday"
    current_sheet_name = now.strftime("%Y-%m") # 例: "2025-01"
    current_day_of_month = now.day            # 例: 26
    return current_day, current_sheet_name, current_day_of_month


def map_date_to_column(day_of_month):
    # 日付を列番号にマッピング (例: 1 -> 3, 26 -> 28)
    return day_of_month + 2


def get_student_indices(student_indices_str):
    # "E523, E534" のような文字列をカンマ区切りでリストに変換
    return [s.strip() for s in student_indices_str.split(",")]


# ---------------------
# メイン処理
# ---------------------
def main():
    current_day, current_sheet_name, current_day_of_month = get_current_date_details()
    print(f"[Debug] Current day: {current_day}")
    print(f"[Debug] Current sheet name: {current_sheet_name}")
    print(f"[Debug] Current day of month: {current_day_of_month}")

    # 1. Courses一覧を取得
    courses_data = get_data_from_firebase("Courses/course_id")
    if not courses_data:
        print(f"[Debug] No courses found.")
        return

    # 2. 現在の曜日と一致するコースをフィルタリング
    matched_courses = []
    for idx, course_info in enumerate(courses_data):
        course_id = idx  # リストのインデックスが course_id
        if course_id == 0:
            continue  # インデックス0は無視
        if not course_info:
            print(f"[Debug] Course data at index {course_id} is None.")
            continue

        schedule_day = course_info.get("schedule", {}).get("day")
        if schedule_day == current_day:
            matched_courses.append((course_id, course_info))
            print(f"[Debug] Course {course_id} matches the current day.")

    if not matched_courses:
        print(f"[Debug] No courses match the current day.")
        return

    # 3. 各マッチしたコースに対して処理
    for course_id, course_info in matched_courses:
        print(f"[Debug]\nProcessing Course ID: {course_id}, Course Name: {course_info.get('course_name')}")

        # 4. Enrollmentからstudent_indicesを取得
        enrollment_path = f"Students/enrollment/course_id/{course_id}/student_index"
        student_indices_str = get_data_from_firebase(enrollment_path)
        if not student_indices_str:
            print(f"[Debug] No students enrolled in course {course_id}.")
            continue

        student_indices = get_student_indices(student_indices_str)
        print(f"[Debug] Student indices for course {course_id}: {student_indices}")

        # 5. 各student_indexに対して処理
        for idx, student_idx in enumerate(student_indices, start=1):
            row_number = idx + 1  # ヘッダーが1行目にある場合、最初の学生は2行目
            print(f"[Debug]\nProcessing Student {student_idx} (List Index: {idx}, Sheet Row: {row_number})")

            # 6. student_idを取得
            student_info_path = f"Students/student_info/student_index/{student_idx}/student_id"
            student_id = get_data_from_firebase(student_info_path)
            if not student_id:
                print(f"[Debug] No student_id found for student_index {student_idx}.")
                continue
            print(f"[Debug] Student ID: {student_id}")

            # 7. attendanceからdecisionを取得
            decision_path = f"Students/attendance/student_id/{student_id}/course_id/{course_id}/decision"
            decision = get_data_from_firebase(decision_path)
            if decision is None:
                print(f"[Debug] No decision found for student_id {student_id} in course {course_id}.")
                continue
            print(f"[Debug] Decision: {decision}")

            # 8. course_sheet_idを取得
            sheet_id = course_info.get("course_sheet_id")
            if not sheet_id:
                print(f"[Debug] No course_sheet_id found for course {course_id}.")
                continue
            print(f"[Debug] Course Sheet ID: {sheet_id}")

            try:
                # 9. Google Sheetを開く
                sh = gclient.open_by_key(sheet_id)
                print(f"[Debug] Opened Google Sheet: {sh.title}")

                # シート名を動的に指定 (例: "2025-01")
                try:
                    sheet = sh.worksheet(current_sheet_name)
                    print(f"[Debug] Using worksheet: {sheet.title}")
                except gspread.exceptions.WorksheetNotFound:
                    print(f"[Debug] Worksheet named '{current_sheet_name}' not found in spreadsheet {sheet_id}.")
                    continue

                # 列を決定
                column = map_date_to_column(current_day_of_month)
                print(f"[Debug] Mapped day of month '{current_day_of_month}' to column {column}.")

                # セルにdecisionを入力
                sheet.update_cell(row_number, column, decision)
                print(f"[Debug] Updated cell at row {row_number}, column {column} with decision '{decision}'.")

            except gspread.exceptions.SpreadsheetNotFound:
                print(f"[Debug] Spreadsheet with ID {sheet_id} not found.")
            except gspread.exceptions.WorksheetNotFound:
                print(f"[Debug] Worksheet '{current_sheet_name}' not found in spreadsheet {sheet_id}.")
            except Exception as e:
                print(f"[Debug] Error updating Google Sheet for course {course_id}, student {student_idx}: {e}")


if __name__ == "__main__":
    main()
