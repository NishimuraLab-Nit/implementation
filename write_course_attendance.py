import datetime
import firebase_admin
from firebase_admin import credentials, db
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------
# Firebase & GSpread初期化
# ---------------------
def initialize_firebase():
    if not firebase_admin._apps:
        try:
            cred = credentials.Certificate('/tmp/firebase_service_account.json')
            firebase_admin.initialize_app(cred, {
                'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
            })
            print("[DEBUG] Firebase initialized successfully.")
        except Exception as e:
            print(f"[ERROR] Failed to initialize Firebase: {e}")
    else:
        print("[DEBUG] Firebase already initialized.")

def initialize_gspread():
    try:
        scope = ["https://spreadsheets.google.com/feeds", 
                 "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
        gclient = gspread.authorize(creds)
        print("[DEBUG] Google Sheets API authorized successfully.")
        return gclient
    except Exception as e:
        print(f"[ERROR] Failed to authorize Google Sheets API: {e}")
        return None

# ---------------------
# Firebaseアクセス関連
# ---------------------
def get_data_from_firebase(path):
    print(f"[DEBUG] Fetching data from Firebase path: {path}")
    try:
        ref = db.reference(path)
        data = ref.get()
        if data is None:
            print(f"[DEBUG] No data found at path: {path}")
        else:
            print(f"[DEBUG] Data fetched successfully from path: {path}")
        return data
    except Exception as e:
        print(f"[ERROR] Error fetching data from Firebase path {path}: {e}")
        return None

# ---------------------
# メイン処理
# ---------------------
def process_attendance_and_write_sheet():
    print("[INFO] Starting attendance processing...")
    
    # Firebaseからデータを取得
    courses_data = get_data_from_firebase("Courses/course_id")
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    student_info_data = get_data_from_firebase("Students/student_info")
    
    print(f"[DEBUG] Courses data fetched: {len(courses_data) if courses_data else 0} entries.")
    print(f"[DEBUG] Attendance data fetched: {len(attendance_data) if attendance_data else 0} entries.")
    print(f"[DEBUG] Student info data fetched: {len(student_info_data) if student_info_data else 0} entries.")
    
    if not courses_data or not attendance_data or not student_info_data:
        print("[ERROR] 必要なデータが不足しています。処理を中断します。")
        return
    
    gclient = initialize_gspread()
    if not gclient:
        print("[ERROR] Google Sheetsクライアントの初期化に失敗しました。処理を中断します。")
        return
    
    for course_id, course_info in courses_data.items():
        print(f"[DEBUG] Processing course_id: {course_id}")
        if not course_info:
            print(f"[WARNING] course_infoが存在しません。course_id: {course_id} をスキップします。")
            continue
        
        course_sheet_id = course_info.get("course_sheet_id")
        schedule = course_info.get("schedule", {})
        day = schedule.get("day")
        time_range = schedule.get("time")
        
        print(f"[DEBUG] Course ID: {course_id}, Sheet ID: {course_sheet_id}, Day: {day}, Time Range: {time_range}")
        
        if not course_sheet_id:
            print(f"[WARNING] course_sheet_idが無効です。course_id: {course_id} をスキップします。")
            continue
        
        try:
            sheet = gclient.open_by_key(course_sheet_id)
            print(f"[DEBUG] Opened Google Sheet with ID: {course_sheet_id}")
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"[ERROR] シートが見つかりません: {course_sheet_id}")
            continue
        except Exception as e:
            print(f"[ERROR] Google Sheetを開く際にエラーが発生しました: {e}")
            continue
        
        sheet_name = datetime.datetime.now().strftime("%Y-%m")
        try:
            worksheet = sheet.worksheet(sheet_name)
            print(f"[DEBUG] Found existing worksheet: {sheet_name}")
        except gspread.exceptions.WorksheetNotFound:
            print(f"[DEBUG] Worksheet '{sheet_name}' が見つかりません。新規作成します。")
            try:
                worksheet = sheet.add_worksheet(title=sheet_name, rows=100, cols=31)
                print(f"[DEBUG] Worksheet '{sheet_name}' を新規作成しました。")
            except Exception as e:
                print(f"[ERROR] Worksheet '{sheet_name}' の作成に失敗しました: {e}")
                continue
        except Exception as e:
            print(f"[ERROR] Worksheet '{sheet_name}' の取得中にエラーが発生しました: {e}")
            continue
        
        student_row_map = {}
        row_counter = 2  # Row 1 is header
        total_students = len(attendance_data)
        processed_students = 0
        
        print(f"[DEBUG] Starting to process {total_students} students for course_id: {course_id}")
        
        for student_id, student_attendance in attendance_data.items():
            print(f"[DEBUG] Processing student_id: {student_id} ({processed_students + 1}/{total_students})")
            
            student_info = student_info_data.get(student_id, {})
            student_index = student_info.get("student_index")
            if not student_index:
                print(f"[WARNING] student_indexが見つかりません。student_id: {student_id} をスキップします。")
                continue
            
            enrollment_path = f"Students/enrollment/student_index/{student_index}"
            enrollment_data = get_data_from_firebase(enrollment_path)
            if not enrollment_data:
                print(f"[WARNING] enrollment_dataが見つかりません。student_index: {student_index} をスキップします。")
                continue
            
            enrolled_courses = enrollment_data.get("course_id", "").split(",")
            print(f"[DEBUG] Student {student_id} is enrolled in courses: {enrolled_courses}")
            if str(course_id) not in enrolled_courses:
                print(f"[DEBUG] Student {student_id} は course_id: {course_id} に登録されていません。スキップします。")
                continue
            
            if student_index not in student_row_map:
                student_row_map[student_index] = row_counter
                print(f"[DEBUG] Assigned row {row_counter} to student_index: {student_index}")
                row_counter += 1
            
            # 判定結果をFirebaseから取得
            decision_path = f"Students/attendance/student_id/{student_id}/course_id/{course_id}/decision"
            decision_data = get_data_from_firebase(decision_path)
            if not decision_data:
                print(f"[WARNING] Decision dataが見つかりません。student_id: {student_id}, course_id: {course_id} をスキップします。")
                continue
            
            total_decisions = len(decision_data)
            print(f"[DEBUG] Found {total_decisions} decision entries for student_id: {student_id}, course_id: {course_id}")
            
            for idx, (date_str, decision) in enumerate(decision_data.items(), start=1):
                print(f"[DEBUG] Processing decision {idx}/{total_decisions} for date: {date_str}")
                try:
                    # 日付文字列を直接解析して日付オブジェクトを取得
                    entry_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                    print(f"[DEBUG] Parsed date_str '{date_str}' to date object: {entry_date}")
                except ValueError:
                    print(f"[ERROR] Invalid date format in decision data: {date_str}. Skipping this entry.")
                    continue
                
                # シートの列を日付に基づいて計算（1列目を日付1日に対応）
                col = entry_date.day + 1
                row = student_row_map[student_index] + 1
                status = decision  # 取得した判定結果を使用
                
                print(f"[DEBUG] Preparing to write status '{status}' to sheet at row {row}, column {col} (Date: {entry_date})")
                
                try:
                    current_value = worksheet.cell(row, col).value
                    print(f"[DEBUG] Current value at row {row}, column {col}: '{current_value}'")
                    if current_value != status:
                        worksheet.update_cell(row, col, status)
                        print(f"[INFO] Updated cell at row {row}, column {col} with status '{status}'.")
                    else:
                        print(f"[DEBUG] Status '{status}' already present at row {row}, column {col}. No update needed.")
                except Exception as e:
                    print(f"[ERROR] Failed to write to sheet at row {row}, column {col}. Error: {e}")
            
            processed_students += 1
            print(f"[DEBUG] Completed processing for student_id: {student_id} ({processed_students}/{total_students})")
    
    if __name__ == "__main__":
        try:
            initialize_firebase()
            process_attendance_and_write_sheet()
        except Exception as e:
            print(f"[CRITICAL] An unexpected error occurred: {e}")
