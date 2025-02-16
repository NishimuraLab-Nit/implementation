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
        {"databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/"},
    )
    print("[Debug] Firebase initialized.")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/gcp_service_account.json", scope)
gclient = gspread.authorize(creds)
print("[Debug] Google Sheets API authorized.")


def get_data_from_firebase(path):
    """
    Firebase Realtime Database から指定パスのデータを取得します。
    """
    print(f"[Debug] Fetching data from Firebase path: {path}")
    ref = db.reference(path)
    data = ref.get()
    if data is None:
        print(f"[Debug] No data found at path: {path}")
    return data


def get_current_date_details():
    """
    現在の日時を取得し、曜日・シート名・日付を返します。
    """
    now = datetime.datetime.now()
    current_day = now.strftime("%A")           # 例: "Sunday"
    current_sheet_name = now.strftime("%Y-%m")  # 例: "2025-01"
    current_day_of_month = now.day               # 例: 26
    return current_day, current_sheet_name, current_day_of_month


def map_date_to_column(day_of_month):
    """
    日付(1～31)から列番号を決定します。例として day_of_month+2 など。
    """
    return day_of_month + 2


def get_student_indices(student_indices_str):
    """
    文字列 "E523, E534" をカンマで分割してリストに変換します。
    """
    return [s.strip() for s in student_indices_str.split(",")]


def main():
    current_day, current_sheet_name, current_day_of_month = get_current_date_details()
    print(f"[Debug] Current day: {current_day}")
    print(f"[Debug] Current sheet name: {current_sheet_name}")
    print(f"[Debug] Current day of month: {current_day_of_month}")

    # 1. コース一覧を取得
    courses_data = get_data_from_firebase("Courses/course_id")
    if not courses_data:
        print("[Debug] No courses found.")
        return

    # 2. 現在の曜日と一致するコースを抽出
    matched_courses = []
    for idx, course_info in enumerate(courses_data):
        course_id = idx
        # ここで course_id == 0 の場合も処理対象に含めるため、continueは削除します。
        if not course_info:
            print(f"[Debug] Course data at index {course_id} is None.")
            continue

        schedule_day = course_info.get("schedule", {}).get("day")
        if schedule_day == current_day:
            matched_courses.append((course_id, course_info))
            print(f"[Debug] Course {course_id} matches the current day.")

    if not matched_courses:
        print("[Debug] No courses match the current day.")
        return

    # 3. 一致するコースについて処理
    for course_id, course_info in matched_courses:
        print(f"[Debug]\nProcessing Course ID: {course_id}, Course Name: {course_info.get('course_name')}")
        enrollment_path = f"Students/enrollment/course_id/{course_id}/student_index"
        student_indices_str = get_data_from_firebase(enrollment_path)
        if not student_indices_str:
            print(f"[Debug] No students enrolled in course {course_id}.")
            continue

        student_indices = get_student_indices(student_indices_str)
        print(f"[Debug] Student indices for course {course_id}: {student_indices}")

        # 4. 各学生について処理
        for idx, student_idx in enumerate(student_indices, start=1):
            row_number = idx + 1
            print(f"[Debug]\nProcessing Student {student_idx} (List Index: {idx}, Sheet Row: {row_number})")

            student_info_path = f"Students/student_info/student_index/{student_idx}/student_id"
            student_id = get_data_from_firebase(student_info_path)
            if not student_id:
                print(f"[Debug] No student_id found for student_index {student_idx}.")
                continue
            print(f"[Debug] Student ID: {student_id}")

            decision_path = f"Students/attendance/student_id/{student_id}/course_id/{course_id}/decision"
            decision = get_data_from_firebase(decision_path)
            if decision is None:
                print(f"[Debug] No decision found for student_id {student_id} in course {course_id}.")
                continue
            print(f"[Debug] Decision: {decision}")

            sheet_id = course_info.get("course_sheet_id")
            if not sheet_id:
                print(f"[Debug] No course_sheet_id found for course {course_id}.")
                continue
            print(f"[Debug] Course Sheet ID: {sheet_id}")

            try:
                sh = gclient.open_by_key(sheet_id)
                print(f"[Debug] Opened Google Sheet: {sh.title}")

                try:
                    sheet = sh.worksheet(current_sheet_name)
                    print(f"[Debug] Using worksheet: {sheet.title}")
                except gspread.exceptions.WorksheetNotFound:
                    print(f"[Debug] Worksheet named '{current_sheet_name}' not found in spreadsheet {sheet_id}.")
                    continue

                column = map_date_to_column(current_day_of_month)
                print(f"[Debug] Mapped day of month '{current_day_of_month}' to column {column}.")
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
