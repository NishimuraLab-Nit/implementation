from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp
import httplib2
import time
import socket
from datetime import datetime, timedelta


def initialize_firebase():
    """
    Firebaseを初期化します。
    """
    firebase_cred = credentials.Certificate("firebase-adminsdk.json")
    initialize_app(
        firebase_cred,
        {
            "databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/",
        },
    )
    print("[Debug] Firebase initialized.")


def get_google_sheets_service():
    """
    Google Sheets APIサービスを初期化し、リトライ付きHTTPクライアントを設定して返します。
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    authorized_http = AuthorizedHttp(google_creds, http=httplib2.Http(timeout=60))
    service = build("sheets", "v4", cache_discovery=False, http=authorized_http)
    print("[Debug] Google Sheets API service initialized.")
    return service


def get_firebase_data(ref_path):
    """
    Firebaseから指定パスのデータを取得して返します。
    """
    try:
        print(f"[Debug] Fetching data from Firebase path: {ref_path}")
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"[Debug] Firebaseデータ取得エラー: {e}")
        return None


def execute_with_retry(request, retries=3, delay=5):
    """
    HttpError や socket.timeout が発生した場合にリトライするヘルパー関数。
    """
    for attempt in range(retries):
        try:
            return request.execute()
        except (HttpError, socket.timeout) as e:
            print(f"[Debug] リクエスト失敗 ({attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise


def get_sheet_id(course_id):
    """
    コースIDに紐づくスプレッドシートIDをFirebaseから取得します。
    """
    course_data = get_firebase_data(f"Courses/course_id/{course_id}")
    if course_data and "course_sheet_id" in course_data:
        return course_data["course_sheet_id"]
    print(f"[Debug] Course ID {course_id} の course_sheet_id が見つかりません。")
    return None


def get_students_by_course(course_id):
    """
    指定したコースを履修している学生の名前リストと出席番号リストを取得して返します。
    """
    enrollment_data = get_firebase_data(f"Students/enrollment/course_id/{course_id}")
    if not enrollment_data or "student_index" not in enrollment_data:
        print(f"[Debug] Course ID {course_id} の学生データが見つかりません。")
        return [], []

    student_indices = enrollment_data["student_index"].split(",")
    student_names = []
    attendance_numbers = []

    for student_index in student_indices:
        student_index_str = student_index.strip()
        student_info = get_firebase_data(f"Students/student_info/student_index/{student_index_str}")
        if student_info:
            student_name = student_info.get("student_name")
            attendance_number = student_info.get("attendance_number")
            if student_name:
                student_names.append(student_name)
                attendance_numbers.append(attendance_number or "")
            else:
                print(f"[Debug] 学生インデックス {student_index_str} の名前が見つかりません。")
        else:
            print(f"[Debug] 学生インデックス {student_index_str} の情報が見つかりません。")

    return student_names, attendance_numbers


def create_dimension_request(sheet_id, dimension, start_index, end_index, pixel_size):
    """
    シートの行または列のサイズを設定するリクエストを作成します。
    """
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": dimension,
                "startIndex": start_index,
                "endIndex": end_index,
            },
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize",
        }
    }


def create_cell_update_request(sheet_id, row, col, value):
    """
    シートの特定セルに文字列を書き込むリクエストを作成します。
    """
    return {
        "updateCells": {
            "rows": [
                {
                    "values": [
                        {
                            "userEnteredValue": {"stringValue": str(value)}
                        }
                    ]
                }
            ],
            "fields": "userEnteredValue",
            "start": {
                "sheetId": sheet_id,
                "rowIndex": row,
                "columnIndex": col,
            },
        }
    }


def create_weekend_color_request(sheet_id, start_row, end_row, start_col, end_col, color):
    """
    土曜・日曜など休日用セルに色を塗るリクエストを作成します。
    """
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col,
            },
            "cell": {"userEnteredFormat": {"backgroundColor": color}},
            "fields": "userEnteredFormat.backgroundColor",
        }
    }


def create_black_background_request(sheet_id, start_row, end_row, start_col, end_col):
    """
    不要領域を黒背景に設定するリクエストを作成します。
    """
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col,
            },
            "cell": {
                "userEnteredFormat": {"backgroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}}
            },
            "fields": "userEnteredFormat.backgroundColor",
        }
    }


def prepare_update_requests(sheet_id, student_names, attendance_numbers, month, sheets_service, spreadsheet_id, year=2025):
    """
    1つの月シートを作成し、学生名・出席番号を入力し、日付列を作成するためのリクエストを組み立てます。
    """
    if not student_names:
        print("[Debug] 学生名リストが空です。")
        return []

    # シート作成リクエストをまず追加
    base_title = f"{year}-{str(month).zfill(2)}"
    add_sheet_request = {
        "addSheet": {
            "properties": {
                "title": base_title
            }
        }
    }

    # シートを追加してIDを取得
    requests = [add_sheet_request]
    print(f"[Debug] Adding new sheet titled '{base_title}'.")
    response = execute_with_retry(
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        )
    )

    new_sheet_id = next(
        (reply["addSheet"]["properties"]["sheetId"] for reply in response.get("replies", []) if "addSheet" in reply),
        None,
    )
    if new_sheet_id is None:
        print("[Debug] 新しいシートのIDを取得できませんでした。")
        return []

    # 列・行幅などを設定
    requests = [
        {
            "appendDimension": {
                "sheetId": new_sheet_id,
                "dimension": "COLUMNS",
                "length": 35,
            }
        },
        create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 30),
        create_dimension_request(new_sheet_id, "COLUMNS", 1, 2, 100),
        create_dimension_request(new_sheet_id, "COLUMNS", 2, 35, 35),
        create_dimension_request(new_sheet_id, "ROWS", 0, 1, 120),
        create_dimension_request(new_sheet_id, "ROWS", 1, 35, 30),
        {
            "repeatCell": {
                "range": {
                    "sheetId": new_sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 35,
                    "startColumnIndex": 0,
                    "endColumnIndex": 35,
                },
                "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                "fields": "userEnteredFormat.horizontalAlignment",
            }
        },
        {
            "updateBorders": {
                "range": {
                    "sheetId": new_sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 35,
                    "startColumnIndex": 0,
                    "endColumnIndex": 35,
                },
                "top": {"style": "SOLID", "width": 1},
                "bottom": {"style": "SOLID", "width": 1},
                "left": {"style": "SOLID", "width": 1},
                "right": {"style": "SOLID", "width": 1},
            }
        },
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": new_sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 35,
                        "startColumnIndex": 0,
                        "endColumnIndex": 35,
                    }
                }
            }
        },
    ]

    # 学生名・出席番号をセット
    print("[Debug] Writing student names and attendance numbers...")
    requests.append(create_cell_update_request(new_sheet_id, 0, 1, "学生名"))
    requests.append(create_cell_update_request(new_sheet_id, 0, 0, "AN"))

    for i, (name, attendance_number) in enumerate(zip(student_names, attendance_numbers)):
        requests.append(create_cell_update_request(new_sheet_id, i + 1, 0, attendance_number))
        requests.append(create_cell_update_request(new_sheet_id, i + 1, 1, name))

    # 日付を設定
    print("[Debug] Setting dates...")
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    from datetime import datetime, timedelta
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    current_date = start_date
    start_column = 2

    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = (
            f"{current_date.strftime('%m')}\n月\n"
            f"{current_date.strftime('%d')}\n日\n⌢\n"
            f"{japanese_weekdays[weekday]}\n⌣"
        )
        requests.append(create_cell_update_request(new_sheet_id, 0, start_column, date_string))

        if weekday == 5:
            color = {"red": 0.8, "green": 0.9, "blue": 1.0}
            requests.append(create_weekend_color_request(new_sheet_id, 0, 35, start_column, start_column + 1, color))
        elif weekday == 6:
            color = {"red": 1.0, "green": 0.8, "blue": 0.8}
            requests.append(create_weekend_color_request(new_sheet_id, 0, 35, start_column, start_column + 1, color))

        start_column += 1
        current_date += timedelta(days=1)

    # 不要領域を黒背景に
    print("[Debug] Setting background color for unused cells...")
    requests.append(create_black_background_request(new_sheet_id, 35, 1000, 0, 1000))
    requests.append(create_black_background_request(new_sheet_id, 0, 1000, 35, 1000))

    return requests


def main():
    print("[Debug] Initializing Firebase and Google Sheets...")
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    print("[Debug] Fetching Courses data...")
    courses = get_firebase_data("Courses/course_id")
    if not courses or not isinstance(courses, list):
        print("[Debug] Courses データが見つかりません。")
        return

    # ここを 1 から -> 0 からに変更
    for course_id in range(0, len(courses)):
        print(f"[Debug] Processing course_id={course_id}")
        spreadsheet_id = get_sheet_id(course_id)
        if not spreadsheet_id:
            continue

        student_names, attendance_numbers = get_students_by_course(course_id)
        if not student_names:
            print(f"[Debug] No student names found for course_id={course_id}. Skipping.")
            continue

        for month in range(1, 13):
            print(f"[Debug] Preparing requests for month={month}, course_id={course_id}")
            requests = prepare_update_requests(
                sheet_id=spreadsheet_id,
                student_names=student_names,
                attendance_numbers=attendance_numbers,
                month=month,
                sheets_service=sheets_service,
                spreadsheet_id=spreadsheet_id
            )
            if requests:
                print(f"[Debug] Executing batchUpdate for month={month}, course_id={course_id} ...")
                execute_with_retry(
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={"requests": requests},
                    )
                )
                print(f"[Debug] Sheet for month={month} updated successfully.")
            else:
                print(f"[Debug] No requests to update for month={month} (course_id={course_id}).")


if __name__ == "__main__":
    main()
