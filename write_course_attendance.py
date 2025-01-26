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
        print("[DEBUG] Initializing Firebase...")
        cred = credentials.Certificate('/tmp/firebase_service_account.json')
        firebase_admin.initialize_app(cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })
        print("[DEBUG] Firebase initialized successfully.")
    else:
        print("[DEBUG] Firebase already initialized.")

def initialize_gspread():
    print("[DEBUG] Authorizing Google Sheets API...")
    scope = ["https://spreadsheets.google.com/feeds", 
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/gcp_service_account.json', scope)
    gclient = gspread.authorize(creds)
    print("[DEBUG] Google Sheets API authorized successfully.")
    return gclient

# ---------------------
# Firebaseアクセス関連
# ---------------------
def get_data_from_firebase(path):
    print(f"[DEBUG] Fetching data from Firebase path: {path}")
    ref = db.reference(path)
    data = ref.get()
    if data is None:
        print(f"[DEBUG] No data found at path: {path}")
    else:
        print(f"[DEBUG] Data fetched successfully from path: {path}")
    return data

# ---------------------
# メイン処理
# ---------------------
def process_attendance_and_write_sheet(gclient):
    print("[INFO] Starting attendance processing...")
    
    # Firebaseからデータを取得
    courses_data = get_data_from_firebase("Courses/course_id")
    attendance_data = get_data_from_firebase("Students/attendance/student_id")
    student_info_data = get_data_from_firebase("Students/student_info")
    
    if courses_data:
        print(f"[DEBUG] Courses data fetched: {len(courses_data)} entries.")
    else:
        print("[DEBUG] No courses data fetched.")
    
    if attendance_data:
        print(f"[DEBUG] Attendance data fetched: {len(attendance_data)} entries.")
    else:
        print("[DEBUG] No attendance data fetched.")
    
    if student_info_data:
        print(f"[DEBUG] Student info data fetched: {len(student_info_data)} entries.")
    else:
        print("[DEBUG] No student info data fetched.")
    
    if not courses_data or not attendance_data or not student_info_data:
        print("[ERROR] 必要なデータが不足しています。処理を中断します。")
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
        
        print(f"[DEBUG] Opening Google Sheet with ID: {course_sheet_id}")
        sheet = gclient.open_by_key(course_sheet_id)
        print(f"[DEBUG] Opened Google Sheet with ID: {course_sheet_id}")
        
        sheet_name = datetime.datetime.now().strftime("%Y-%m")
        print(f"[DEBUG] Looking for worksheet: {sheet_name}")
        
        # シート名が存在するか確認
        worksheet_titles = [ws.title for ws in sheet.worksheets()]
        print(f"[DEBUG] Existing worksheets: {worksheet_titles}")
        
        if sheet_name in worksheet_titles:
            print(f"[DEBUG] Found existing worksheet: {sheet_name}")
            worksheet = sheet.worksheet(sheet_name)
        else:
            print(f"[DEBUG] Worksheet '{sheet_name}' が見つかりません。新規作成します。")
            worksheet = sheet.add_worksheet(title=sheet_name, rows=100, cols=31)
            print(f"[DEBUG] Worksheet '{sheet_name}' を新規作成しました。")
        
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
                # 日付文字列を直接解析して日付オブジェクトを取得
                entry_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                print(f"[DEBUG] Parsed date_str '{date_str}' to date object: {entry_date}")
                
                # シートの列を日付に基づいて計算（1列目を日付1日に対応）
                col = entry_date.day + 1
                row = student_row_map[student_index] + 1
                status = decision  # 取得した判定結果を使用
                
                print(f"[DEBUG] Preparing to write status '{status}' to sheet at row {row}, column {col} (Date: {entry_date})")
                
                current_value = worksheet.cell(row, col).value
                print(f"[DEBUG] Current value at row {row}, column {col}: '{current_value}'")
                if current_value != status:
                    worksheet.update_cell(row, col, status)
                    print(f"[INFO] Updated cell at row {row}, column {col} with status '{status}'.")
                else:
                    print(f"[DEBUG] Status '{status}' already present at row {row}, column {col}. No update needed.")
            
            processed_students += 1
            print(f"[DEBUG] Completed processing for student_id: {student_id} ({processed_students}/{total_students})")
    
    if __name__ == "__main__":
        initialize_firebase()
        gclient = initialize_gspread()
        process_attendance_and_write_sheet(gclient)
        print("[INFO] 処理が完了しました。")
