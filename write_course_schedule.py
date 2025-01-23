from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp
import httplib2
import time
import socket
from datetime import datetime, timedelta

# =====================
# Firebaseの初期化
# =====================
def initialize_firebase():
    firebase_cred = credentials.Certificate("firebase-adminsdk.json")
    initialize_app(firebase_cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# =====================
# Google Sheets APIサービスの初期化
# =====================
def get_google_sheets_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    authorized_http = AuthorizedHttp(google_creds, http=httplib2.Http(timeout=60))
    return build('sheets', 'v4', cache_discovery=False, http=authorized_http)

# =====================
# Firebaseからデータを取得
# =====================
def get_firebase_data(ref_path):
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Firebaseデータ取得エラー: {e}")
        return None

# =====================
# リトライ付きリクエスト実行
# =====================
def execute_with_retry(request, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return request.execute()
        except (HttpError, socket.timeout) as e:
            print(f"リクエスト失敗 ({attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise

# =====================
# シートIDを取得
# =====================
def get_sheet_id(course_id):
    course_data = get_firebase_data(f"Courses/course_id/{course_id}")
    if course_data and "course_sheet_id" in course_data:
        return course_data["course_sheet_id"]
    print(f"Course ID {course_id} の course_sheet_id が見つかりません。")
    return None

# =====================
# 学生データを取得
# =====================
def get_students_by_course(course_id):
    enrollment_data = get_firebase_data(f"Students/enrollment/course_id/{course_id}")
    if not enrollment_data or "student_index" not in enrollment_data:
        print(f"Course ID {course_id} の学生データが見つかりません。")
        return [], []

    student_indices = enrollment_data["student_index"].split(",")
    student_names = []
    attendance_numbers = []

    for student_index in student_indices:
        student_info = get_firebase_data(f"Students/student_info/student_index/{student_index.strip()}")
        if student_info:
            student_name = student_info.get("student_name")
            attendance_number = student_info.get("attendance_number")
            if student_name:
                student_names.append(student_name)
                attendance_numbers.append(attendance_number or "")
            else:
                print(f"学生インデックス {student_index} の名前が見つかりません。")
        else:
            print(f"学生インデックス {student_index} の情報が見つかりません。")

    return student_names, attendance_numbers

# =====================
# 以下、ヘルパー関数の定義
# =====================

def create_dimension_request(sheet_id, dimension, start_index, end_index, pixel_size):
    """
    シートの行または列のサイズを設定するリクエストを作成する
    """
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": dimension,  # "ROWS" または "COLUMNS"
                "startIndex": start_index,
                "endIndex": end_index
            },
            "properties": {
                "pixelSize": pixel_size
            },
            "fields": "pixelSize"
        }
    }

def create_cell_update_request(sheet_id, row, col, value):
    """
    シートの特定セルに文字列を設定するリクエストを作成する
    """
    return {
        "updateCells": {
            "rows": [
                {
                    "values": [
                        {
                            "userEnteredValue": {
                                "stringValue": str(value)
                            }
                        }
                    ]
                }
            ],
            "fields": "userEnteredValue",
            "start": {
                "sheetId": sheet_id,
                "rowIndex": row,
                "columnIndex": col
            }
        }
    }

def create_weekend_color_request(sheet_id, start_row, end_row, start_col, end_col, color):
    """
    土曜・日曜などにセルの背景色を付けるリクエストを作成する
    """
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": color
                }
            },
            "fields": "userEnteredFormat.backgroundColor"
        }
    }

def create_black_background_request(sheet_id, start_row, end_row, start_col, end_col):
    """
    使わない領域の背景色を黒に設定するリクエストを作成する
    """
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {
                        "red": 0.0,
                        "green": 0.0,
                        "blue": 0.0
                    }
                }
            },
            "fields": "userEnteredFormat.backgroundColor"
        }
    }

# =====================
# シート更新リクエストを準備
# =====================
def prepare_update_requests(sheet_id, student_names, attendance_numbers, month, sheets_service, spreadsheet_id, year=2025):
    if not student_names:
        print("学生名リストが空です。")
        return []

    # ① 新しいシートを追加
    base_title = f"{year}-{str(month).zfill(2)}"
    add_sheet_request = {
        "addSheet": {
            "properties": {
                "title": base_title
            }
        }
    }

    # シート追加リクエストをまず実行して sheetId を取得
    requests = [add_sheet_request]
    response = execute_with_retry(
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        )
    )
    new_sheet_id = next(
        (reply['addSheet']['properties']['sheetId'] for reply in response.get('replies', []) if 'addSheet' in reply),
        None
    )
    if new_sheet_id is None:
        print("新しいシートのIDを取得できませんでした。")
        return []

    # ② カラム・行幅などを設定するリクエスト
    requests = [
        # 必要な列を確保 (最大 126 列)
        {
            "appendDimension": {
                "sheetId": new_sheet_id,
                "dimension": "COLUMNS",
                "length": 126
            }
        },
        # 出席番号列の幅
        create_dimension_request(new_sheet_id, "COLUMNS", 0, 1, 50),
        # 学生名列の幅
        create_dimension_request(new_sheet_id, "COLUMNS", 1, 2, 150),
        # 日付列(2～126列)の幅
        create_dimension_request(new_sheet_id, "COLUMNS", 2, 126, 80),
        # ヘッダー行の高さ
        create_dimension_request(new_sheet_id, "ROWS", 0, 1, 40),
        # 学生データ行(2～)の高さ (ここでは仮に 35 行分確保)
        create_dimension_request(new_sheet_id, "ROWS", 1, 35, 30),
        # ヘッダー行の背景色(薄いグレー)
        {
            "repeatCell": {
                "range": {
                    "sheetId": new_sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 126
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 0.9,
                            "green": 0.9,
                            "blue": 0.9
                        }
                    }
                },
                "fields": "userEnteredFormat.backgroundColor"
            }
        },
        # テキストの横位置を中央揃え
        {
            "repeatCell": {
                "range": {
                    "sheetId": new_sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 35,
                    "startColumnIndex": 0,
                    "endColumnIndex": 126
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER"
                    }
                },
                "fields": "userEnteredFormat.horizontalAlignment"
            }
        },
        # セルの境界線を設定
        {
            "updateBorders": {
                "range": {
                    "sheetId": new_sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 35,
                    "startColumnIndex": 0,
                    "endColumnIndex": 126
                },
                "top": {
                    "style": "SOLID",
                    "width": 1
                },
                "bottom": {
                    "style": "SOLID",
                    "width": 1
                },
                "left": {
                    "style": "SOLID",
                    "width": 1
                },
                "right": {
                    "style": "SOLID",
                    "width": 1
                }
            }
        },
        # フィルタを適用（列が多いので全体を対象に）
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": new_sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 35,
                        "startColumnIndex": 0,
                        "endColumnIndex": 126
                    }
                }
            }
        }
    ]

    # 学生名・出席番号のヘッダーを入れる
    requests.append(create_cell_update_request(new_sheet_id, 0, 1, "学生名"))
    requests.append(create_cell_update_request(new_sheet_id, 0, 0, "AN"))

    # 学生名・出席番号を各行に記入
    for i, (name, attendance_number) in enumerate(zip(student_names, attendance_numbers)):
        # 出席番号(AN)を0列に、学生名を1列に
        requests.append(create_cell_update_request(new_sheet_id, i + 2, 0, attendance_number))
        requests.append(create_cell_update_request(new_sheet_id, i + 2, 1, name))

    # ③ 日付と授業時限ラベルを設定
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    current_date = start_date
    start_column = 2
    period_labels = ["1,2限", "3,4限", "5,6限", "7,8限"]

    # 各日ごとに日付セルと時限セルを設定
    while current_date <= end_date:
        weekday = current_date.weekday()  # 月曜日=0, 日曜日=6
        # 日付と曜日を改行混じりで作成 (例: 01\n月\n15\n日\n⌢\n水\n⌣)
        date_string = f"{current_date.strftime('%m')}\n月\n{current_date.strftime('%d')}\n日\n⌢\n{japanese_weekdays[weekday]}\n⌣"
        # 日付部分(ヘッダー最上段: row=0)
        requests.append(create_cell_update_request(new_sheet_id, 0, start_column, date_string))

        # 当日の各時限を2行目に配置 (row=1)
        for period_index, period in enumerate(period_labels):
            requests.append(create_cell_update_request(new_sheet_id, 1, start_column + period_index, period))

        # 土曜(weekday=5)・日曜(weekday=6)の背景色
        if weekday == 5:  # 土曜日
            color = {"red": 0.8, "green": 0.9, "blue": 1.0}
            requests.append(
                create_weekend_color_request(new_sheet_id, 0, 35, start_column, start_column + len(period_labels), color)
            )
        elif weekday == 6:  # 日曜日
            color = {"red": 1.0, "green": 0.8, "blue": 0.8}
            requests.append(
                create_weekend_color_request(new_sheet_id, 0, 35, start_column, start_column + len(period_labels), color)
            )

        start_column += len(period_labels)
        current_date += timedelta(days=1)

    # ④ 使わないセルを黒背景に設定
    #    たとえば行は 35 行目以降、列は 126 列目以降を黒にする
    requests.append(create_black_background_request(new_sheet_id, 35, 1000, 0, 1000))
    requests.append(create_black_background_request(new_sheet_id, 0, 1000, 126, 1000))

    return requests

# =====================
# メイン処理
# =====================
def main():
    # Firebase, Google Sheets初期化
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # Firebaseからコース情報を取得
    courses = get_firebase_data("Courses/course_id")
    if not courses or not isinstance(courses, list):
        print("Courses データが見つかりません。")
        return

    # コースIDを 1 からスタート (0 は無視する想定)
    for course_id in range(1, len(courses)):
        spreadsheet_id = get_sheet_id(course_id)  # そのコースに対応したスプレッドシートID
        if not spreadsheet_id:
            continue

        # 学生名と出席番号リストを取得
        student_names, attendance_numbers = get_students_by_course(course_id)
        if not student_names:
            continue

        # 1～12月のシートを作成して更新
        for month in range(1, 13):
            # 更新用リクエストを準備
            requests = prepare_update_requests(
                sheet_id=spreadsheet_id,
                student_names=student_names,
                attendance_numbers=attendance_numbers,
                month=month,
                sheets_service=sheets_service,
                spreadsheet_id=spreadsheet_id
            )
            # リクエストがあれば送信
            if requests:
                execute_with_retry(
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={'requests': requests}
                    )
                )

if __name__ == "__main__":
    main()
