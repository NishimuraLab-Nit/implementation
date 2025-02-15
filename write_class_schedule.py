from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
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
    Google Sheets APIサービスを初期化し、AuthorizedHttp を用いて返します。
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    authorized_http = AuthorizedHttp(google_creds, http=httplib2.Http(timeout=60))
    service = build("sheets", "v4", cache_discovery=False, http=authorized_http)
    print("[Debug] Google Sheets service initialized.")
    return service


def get_firebase_data(ref_path):
    """
    指定パスからFirebaseのデータを取得します。
    """
    try:
        print(f"[Debug] Fetching data from Firebase path: {ref_path}")
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"[Debug] Firebaseデータ取得エラー: {e}")
        return None


def execute_with_retry(request, retries=3, delay=5):
    """
    リクエストを指定回数リトライし、HttpErrorやsocket.timeoutが出ても一定回数まで再試行する。
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


def create_cell_update_request(sheet_id, row_index, column_index, value):
    """
    指定したシートの行列に文字列を設定するためのリクエストを作成します。
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
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue",
        }
    }


def create_sheet_request(sheet_title):
    """
    新しいワークシートを追加するためのリクエストを作成します。
    """
    return {
        "addSheet": {
            "properties": {
                "title": sheet_title,
            }
        }
    }


def create_dimension_request(sheet_id, dimension, start_index, end_index, pixel_size):
    """
    シートの行や列のピクセルサイズを変更するためのリクエストを作成します。
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


def create_weekend_color_request(sheet_id, start_row, end_row, start_col, end_col, color):
    """
    土曜・日曜セルに色付けを行うためのリクエストを作成します。
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
    シートの不要領域を黒色で塗り潰すリクエストを作成します。
    """
    black_color = {"red": 0.0, "green": 0.0, "blue": 0.0}
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col,
            },
            "cell": {"userEnteredFormat": {"backgroundColor": black_color}},
            "fields": "userEnteredFormat.backgroundColor",
        }
    }


def generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title):
    """
    スプレッドシート内で重複しないシート名を生成します。
    """
    print(f"[Debug] Generating unique sheet title for base: {base_title}")
    existing_sheets = execute_with_retry(
        sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id)
    ).get("sheets", [])
    sheet_titles = [sheet["properties"]["title"] for sheet in existing_sheets]

    title = base_title
    counter = 1
    while title in sheet_titles:
        title = f"{base_title} ({counter})"
        counter += 1
    print(f"[Debug] Final sheet title: {title}")
    return title


def get_student_data(class_index):
    """
    指定クラスの student_index に合致する学生の名前・出席番号リストを取得します。
    """
    student_indices = get_firebase_data("Students/student_info/student_index")
    if not student_indices or not isinstance(student_indices, dict):
        print("[Debug] 学生インデックスを取得できませんでした。")
        return [], []

    student_names = []
    attendance_numbers = []

    for index, student_data in student_indices.items():
        if str(index).startswith(class_index):
            student_name = student_data.get("student_name")
            attendance_number = student_data.get("attendance_number")
            if student_name:
                student_names.append(student_name)
                attendance_numbers.append(attendance_number or "")
            else:
                print(f"[Debug] 学生インデックス {index} の名前が見つかりません。")

    return student_names, attendance_numbers


def prepare_update_requests(sheet_id, student_names, attendance_numbers, month, sheets_service, spreadsheet_id, year=2025):
    """
    1つの月用シートを作成し、学生名・出席番号、日付・週末色付けなどを設定するリクエストを返します。
    """
    if not student_names:
        print("[Debug] 学生名リストが空です。Firebaseから取得したデータを確認してください。")
        return []

    base_title = f"{year}-{str(month).zfill(2)}"
    sheet_title = generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title)

    # 新しいシートを追加
    add_sheet_request = create_sheet_request(sheet_title)
    requests = [add_sheet_request]

    print(f"[Debug] Creating new sheet: {sheet_title}")
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

    # 列や行幅の調整
    requests = [
        {"appendDimension": {"sheetId": new_sheet_id, "dimension": "COLUMNS", "length": 126}},
        create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 35),
        create_dimension_request(new_sheet_id, "COLUMNS", 1, 1, 100),
        create_dimension_request(new_sheet_id, "COLUMNS", 2, 126, 35),
        create_dimension_request(new_sheet_id, "ROWS", 0, 1, 120),
        {
            "repeatCell": {
                "range": {"sheetId": new_sheet_id},
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
                    "endColumnIndex": 126,
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
                        "endColumnIndex": 126,
                    }
                }
            }
        },
    ]

    # 学生名・出席番号のヘッダー部分
    print("[Debug] Writing student names and attendance numbers...")
    requests.append(create_cell_update_request(new_sheet_id, 0, 1, "学生名"))
    requests.append(create_cell_update_request(new_sheet_id, 0, 0, "AN"))

    for i, (name, attendance_number) in enumerate(zip(student_names, attendance_numbers)):
        requests.append(create_cell_update_request(new_sheet_id, i + 2, 1, name))
        requests.append(create_cell_update_request(new_sheet_id, i + 2, 0, attendance_number))

    # 日付と授業時限列の設定
    print("[Debug] Setting dates and periods...")
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    current_date = start_date
    start_column = 2
    period_labels = ["1,2限", "3,4限", "5,6限", "7,8限"]

    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = (
            f"{current_date.strftime('%m')}\n月\n"
            f"{current_date.strftime('%d')}\n日\n⌢\n"
            f"{japanese_weekdays[weekday]}\n⌣"
        )
        requests.append(create_cell_update_request(new_sheet_id, 0, start_column, date_string))

        for period_index, period in enumerate(period_labels):
            requests.append(create_cell_update_request(new_sheet_id, 1, start_column + period_index, period))

        if weekday == 5:
            color = {"red": 0.8, "green": 0.9, "blue": 1.0}
            requests.append(
                create_weekend_color_request(
                    new_sheet_id, 0, 35, start_column, start_column + len(period_labels), color
                )
            )
        elif weekday == 6:
            color = {"red": 1.0, "green": 0.8, "blue": 0.8}
            requests.append(
                create_weekend_color_request(
                    new_sheet_id, 0, 35, start_column, start_column + len(period_labels), color
                )
            )

        start_column += len(period_labels)
        current_date += timedelta(days=1)

    # 黒背景設定
    print("[Debug] Setting background color for unused cells...")
    requests.append(create_black_background_request(new_sheet_id, 35, 1000, 0, 1000))
    requests.append(create_black_background_request(new_sheet_id, 0, 1000, 126, 1000))

    return requests


def main():
    print("[Debug] Initializing Firebase and Google Sheets...")
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    print("[Debug] Fetching class indices...")
    class_indices = get_firebase_data("Class/class_index")
    if not class_indices or not isinstance(class_indices, dict):
        print("[Debug] Classインデックスを取得できませんでした。")
        return

    for class_index, class_data in class_indices.items():
        spreadsheet_id = class_data.get("class_sheet_id")
        if not spreadsheet_id:
            print(f"[Debug] クラス {class_index} のスプレッドシートIDが見つかりません。")
            continue

        print(f"[Debug] Fetching student data for class_index={class_index}...")
        student_names, attendance_numbers = get_student_data(class_index)
        if not student_names:
            print(f"[Debug] クラス {class_index} に一致する学生名が見つかりませんでした。")
            continue

        for month in range(1, 13):
            print(f"[Debug] Processing month: {month} for class index: {class_index}")
            requests = prepare_update_requests(
                class_index,
                student_names,
                attendance_numbers,
                month,
                sheets_service,
                spreadsheet_id
            )
            if not requests:
                print(f"[Debug] 月 {month} のシートを更新するリクエストがありません。")
                continue

            print(f"[Debug] Executing batchUpdate for month {month}, class_index={class_index}...")
            execute_with_retry(
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": requests},
                )
            )
            print(f"[Debug] 月 {month} のシートを正常に更新しました。")


if __name__ == "__main__":
    main()
