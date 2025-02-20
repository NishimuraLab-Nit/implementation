from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta


def initialize_firebase():
    """
    Firebase Admin SDK の初期化を行います。
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
    Google Sheets APIサービスを初期化して返します。
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    print("[Debug] Google Sheets API authorized.")
    return build("sheets", "v4", credentials=google_creds)


def get_firebase_data(ref_path):
    """
    Firebaseから指定パスのデータを取得して返します。
    """
    print(f"[Debug] Fetching data from Firebase path: {ref_path}")
    ref = db.reference(ref_path)
    data = ref.get()
    if data is None:
        print(f"[Debug] No data found at path: {ref_path}")
    return data

def execute_with_retry(request):
    # リトライ機能を実装し、スリープを追加
    retries = 3
    for i in range(retries):
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status == 429:  # 429はレート制限を意味します
                print("[デバッグ] レート制限を超えました。再試行します...")
                time.sleep(60)  # 60秒待機して再試行
            else:
                raise e

def create_cell_update_request(sheet_id, row_index, column_index, value):
    """
    シートの特定セルを更新するリクエストを作成します。
    """
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue",
        }
    }


def create_dimension_request(sheet_id, dimension, start_index, end_index, pixel_size):
    """
    シートの列または行のピクセルサイズを設定するリクエストを作成します。
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


def create_black_background_request(sheet_id, start_row, end_row, start_col, end_col):
    """
    指定範囲を黒背景に設定するリクエストを作成します。
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


def create_sheet_request(sheet_title):
    """
    新しいシートを作成するリクエストを作成します。
    """
    return {
        "addSheet": {
            "properties": {
                "title": sheet_title,
                "gridProperties": {
                    "rowCount": 1000,
                    "columnCount": 32,
                },
            }
        }
    }


def get_all_sheets(sheets_service, spreadsheet_id):
    """
    指定スプレッドシートのすべてのワークシート名をリストで返します。
    """
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet.get("sheets", [])
    return [sheet["properties"]["title"] for sheet in sheets]


def generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title):
    """
    シート名の重複を避けるため、base_title が存在する場合は連番を付けて返します。
    """
    existing_titles = get_all_sheets(sheets_service, spreadsheet_id)
    if base_title not in existing_titles:
        return base_title
    index = 1
    while f"{base_title}-{index}" in existing_titles:
        index += 1
    return f"{base_title}-{index}"


def prepare_update_requests(sheet_id, course_names, month, sheets_service, spreadsheet_id, year=2025):
    """
    指定したコース名リストと年月から、スプレッドシート更新リクエストを作成して返します。
    """
    if not course_names:
        print("[Debug] コース名リストが空です。Firebaseから取得したデータを確認してください。")
        return []

    # ユニークなシート名を生成
    base_title = f"{year}-{str(month).zfill(2)}"
    sheet_title = generate_unique_sheet_title(sheets_service, spreadsheet_id, base_title)

    # 新しいシートを追加するリクエスト
    add_sheet_request = create_sheet_request(sheet_title)
    requests = [add_sheet_request]

    # 先にbatchUpdateを実行してシートIDを取得
    response = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()

    new_sheet_id = None
    for reply in response.get("replies", []):
        if "addSheet" in reply:
            new_sheet_id = reply["addSheet"]["properties"]["sheetId"]
    if new_sheet_id is None:
        print("[Debug] 新しいシートのIDを取得できませんでした。")
        return []

    # 追加後のリクエストを再構築
    requests = [
        {"appendDimension": {"sheetId": new_sheet_id, "dimension": "COLUMNS", "length": 32}},
        create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 100),
        create_dimension_request(new_sheet_id, "COLUMNS", 1, 32, 35),
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
                    "endRowIndex": 25,
                    "startColumnIndex": 0,
                    "endColumnIndex": 32,
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
                        "endRowIndex": 25,
                        "startColumnIndex": 0,
                        "endColumnIndex": 32,
                    }
                }
            }
        },
    ]

    # 教科名を設定
    requests.append(create_cell_update_request(new_sheet_id, 0, 0, "教科"))
    for i, name in enumerate(course_names):
        requests.append(create_cell_update_request(new_sheet_id, i + 1, 0, name))

    # 日付と土日セルの色付け
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    end_row = 25
    current_date = start_date

    while current_date <= end_date:
        weekday = current_date.weekday()  # 月=0, 日=6
        date_string = (
            f"{current_date.strftime('%m')}\n月\n"
            f"{current_date.strftime('%d')}\n日\n⌢\n"
            f"{japanese_weekdays[weekday]}\n⌣"
        )
        requests.append(create_cell_update_request(new_sheet_id, 0, current_date.day, date_string))

        # 土曜日(5)・日曜日(6)の背景色を付ける
        if weekday == 5:
            color = {"red": 0.8, "green": 0.9, "blue": 1.0}
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": new_sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": end_row,
                            "startColumnIndex": current_date.day,
                            "endColumnIndex": current_date.day + 1,
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": color}},
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                }
            )
        elif weekday == 6:
            color = {"red": 1.0, "green": 0.8, "blue": 0.8}
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": new_sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": end_row,
                            "startColumnIndex": current_date.day,
                            "endColumnIndex": current_date.day + 1,
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": color}},
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                }
            )
        current_date += timedelta(days=1)

    # 使わない領域を黒背景に
    requests.append(create_black_background_request(new_sheet_id, 25, 1000, 0, 1000))
    requests.append(create_black_background_request(new_sheet_id, 0, 1000, 32, 1000))

    return requests


def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # 学生データの取得
    student_indices = get_firebase_data("Students/student_info/student_index")
    if not student_indices or not isinstance(student_indices, dict):
        print("[Debug] Firebaseから学生インデックスを取得できませんでした。空のデータとして処理を続行します。")
        student_indices = {}

    for student_index, student_data in student_indices.items():
        print(f"[Debug] Processing student index: {student_index}")
        sheet_id = student_data.get("sheet_id")
        if not sheet_id:
            print(f"[Debug] 学生インデックス {student_index} のシートIDが見つかりません。スキップします。")
            continue

        # コースIDを取得
        data = get_firebase_data(f"Students/enrollment/student_index/{student_index}/course_id")
        print(f"[Debug] 取得したデータ (course_id): {data}")

        if isinstance(data, str):
            student_course_ids = [course_id.strip() for course_id in data.split(",")]
        elif isinstance(data, list):
            student_course_ids = [str(course_id).strip() for course_id in data]
        else:
            print(f"[Debug] 学生インデックス {student_index} の登録コースが不正です。スキップします。")
            continue

        print(f"[Debug] 学生インデックス {student_index} の登録コース: {student_course_ids}")

        courses = get_firebase_data("Courses/course_id")
        if not isinstance(courses, list):
            print("[Debug] Courses データが不正です。処理を中止します。")
            continue

        # Coursesデータを辞書化
        courses_dict = {
            str(index): course
            for index, course in enumerate(courses)
            if course is not None and isinstance(course, dict)
        }

        # 学生のコース名リストを作成
        course_names = []
        for cid in student_course_ids:
            if cid in courses_dict:
                course_name = courses_dict[cid].get("course_name")
                if course_name:
                    course_names.append(course_name)
            else:
                print(f"[Debug] コースID {cid} がCoursesデータに存在しません。")

        if not course_names:
            print(f"[Debug] 学生インデックス {student_index} のコース名が見つかりませんでした。")
            continue

        # 各月のシートを作成・更新
        for month in range(1, 13):
            print(f"[Debug] Processing month: {month} for student index: {student_index}")
            requests = prepare_update_requests(sheet_id, course_names, month, sheets_service, sheet_id)
            if not requests:
                print(f"[Debug] 月 {month} のシートを更新するリクエストがありません。")
                continue

            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={"requests": requests},
            ).execute()
            print(f"[Debug] 月 {month} のシートを正常に更新しました。")


if __name__ == "__main__":
    main()
