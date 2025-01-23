from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp
import httplib2
import time
import socket
from datetime import datetime, timedelta

# Firebaseの初期化
def initialize_firebase():
    firebase_cred = credentials.Certificate("firebase-adminsdk.json")
    initialize_app(firebase_cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Google Sheets APIサービスの初期化
def get_google_sheets_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    authorized_http = AuthorizedHttp(google_creds, http=httplib2.Http(timeout=60))
    return build('sheets', 'v4', cache_discovery=False, http=authorized_http)

# Firebaseからデータを取得
def get_firebase_data(ref_path):
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Firebaseデータ取得エラー: {e}")
        return None

# リトライ付きリクエスト実行
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

# シート更新リクエスト作成
def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": str(value)}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }

# シート作成リクエスト
def create_sheet_request(sheet_title):
    return {"addSheet": {"properties": {"title": sheet_title}}}

# 土日セルの色付けリクエストを作成
def create_weekend_color_request(sheet_id, start_row, end_row, start_col, end_col, color):
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col
            },
            "cell": {"userEnteredFormat": {"backgroundColor": color}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    }

# シートの黒背景リクエスト
def create_black_background_request(sheet_id, start_row, end_row, start_col, end_col):
    black_color = {"red": 0.0, "green": 0.0, "blue": 0.0}
    return create_weekend_color_request(sheet_id, start_row, end_row, start_col, end_col, black_color)

# コースIDに基づくシートと学生データの取得
def get_course_sheet_and_student_data(course_id):
    course_data = get_firebase_data(f"Courses/course_id/{course_id}")
    if not course_data:
        print(f"コース {course_id} のデータが見つかりません。")
        return None, []

    sheet_id = course_data.get("course_sheet_id")
    if not sheet_id:
        print(f"コース {course_id} のシートIDが見つかりません。")
        return None, []

    enrollment_data = get_firebase_data(f"Students/enrollment/course_id/{course_id}/student_index")
    if not enrollment_data:
        print(f"コース {course_id} に登録された学生データが見つかりません。")
        return sheet_id, []

    student_indices = enrollment_data.split(", ")
    student_names = []
    for student_index in student_indices:
        student_info = get_firebase_data(f"Students/student_info/student_index/{student_index}")
        if student_info and "student_name" in student_info:
            student_names.append(student_info["student_name"])
        else:
            print(f"学生インデックス {student_index} に対応する名前が見つかりません。")
    return sheet_id, student_names

# シート更新リクエスト準備
def prepare_update_requests(sheets_service, spreadsheet_id, sheet_title, student_names, month, year=2025):
    """シート更新リクエストを準備"""
    # シートIDを取得または新規作成
    sheet_id = get_sheet_id(sheets_service, spreadsheet_id, sheet_title)
    if not sheet_id:
        print(f"シート '{sheet_title}' が見つかりません。新規作成します。")
        sheet_id = create_and_get_sheet_id(sheets_service, spreadsheet_id, sheet_title)
        if not sheet_id:
            print(f"シート '{sheet_title}' の作成に失敗しました。")
            return []

    # リクエストの準備
    requests = []

    # 学生名を記載
    requests.append(create_cell_update_request(sheet_id, 0, 0, "学生名"))
    for i, name in enumerate(student_names):
        requests.append(create_cell_update_request(sheet_id, i + 1, 0, name))

    # 日付と曜日を記載
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    current_date = start_date
    column = 1

    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = f"{current_date.strftime('%m/%d')} ({['月', '火', '水', '木', '金', '土', '日'][weekday]})"
        requests.append(create_cell_update_request(sheet_id, 0, column, date_string))
        if weekday in [5, 6]:  # 土日
            color = {"red": 0.9, "green": 0.9, "blue": 1.0} if weekday == 5 else {"red": 1.0, "green": 0.9, "blue": 0.9}
            requests.append(create_weekend_color_request(sheet_id, 0, len(student_names) + 1, column, column + 1, color))
        column += 1
        current_date += timedelta(days=1)

    return requests

# メイン処理
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # Firebaseからクラス情報を取得
    class_indices = get_firebase_data("Class/class_index")
    if not class_indices or not isinstance(class_indices, dict):
        print("クラス情報を取得できませんでした。")
        return

    # 各クラスについて処理
    for class_index, class_data in class_indices.items():
        course_ids = class_data.get("course_id", "")
        if not course_ids:
            print(f"クラス {class_index} にコースIDが設定されていません。")
            continue

        # コースIDをリストに変換
        course_ids = [int(cid.strip()) for cid in course_ids.split(",")]

        for course_id in course_ids:
            # シートIDと学生データを取得
            sheet_id, student_names = get_course_sheet_and_student_data(course_id)
            if not sheet_id or not student_names:
                print(f"コース {course_id} のデータが不足しています。")
                continue

            # 各月についてシートを準備
            for month in range(1, 13):
                sheet_title = f"{class_index}-{course_id}-{month:02d}"  # シートのタイトルを作成
                requests = prepare_update_requests(
                    sheets_service,  # Google Sheetsサービス
                    sheet_id,        # スプレッドシートID
                    sheet_title,     # シートタイトル
                    student_names,   # 学生名リスト
                    month,           # 月
                )

                if not requests:
                    print(f"月 {month} のシートを更新するリクエストがありません。")
                    continue

                # Google Sheetsにリクエストを送信
                execute_with_retry(
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=sheet_id,
                        body={"requests": requests}
                    )
                )
                print(f"月 {month} のシートを正常に更新しました。")

if __name__ == "__main__":
    main()