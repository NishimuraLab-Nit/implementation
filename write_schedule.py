from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta


def initialize_firebase():
    """Firebaseの初期化"""
    firebase_cred = credentials.Certificate("firebase-adminsdk.json")
    initialize_app(firebase_cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })


def get_google_sheets_service():
    """Google Sheets APIのサービスを取得"""
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
    return build('sheets', 'v4', credentials=google_creds)


def get_firebase_data(ref_path):
    """Firebaseからデータを取得"""
    try:
        return db.reference(ref_path).get()
    except Exception as e:
        print(f"Error retrieving data from Firebase: {e}")
        return None


def create_cell_update_request(sheet_id, row_index, column_index, value):
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }


def create_dimension_request(sheet_id, dimension, start_index, end_index, pixel_size):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": dimension, "startIndex": start_index, "endIndex": end_index},
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize"
        }
    }


def create_conditional_formatting_request(sheet_id, start_row, end_row, start_col, end_col, color, formula):
    return {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                            "startColumnIndex": start_col, "endColumnIndex": end_col}],
                "booleanRule": {
                    "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": formula}]},
                    "format": {"backgroundColor": color}
                }
            },
            "index": 0
        }
    }


def create_black_background_request(sheet_id, start_row, end_row, start_col, end_col):
    black_color = {"red": 0.0, "green": 0.0, "blue": 0.0}
    return {
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                      "startColumnIndex": start_col, "endColumnIndex": end_col},
            "cell": {"userEnteredFormat": {"backgroundColor": black_color}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    }


def create_monthly_sheets(sheet_id, sheets_service):
    """1月～12月のシートを作成"""
    months = ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"]
    requests = [{"addSheet": {"properties": {"title": month}}} for month in months]

    try:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": requests}
        ).execute()
        print("Monthly sheets created successfully.")
    except Exception as e:
        print(f"Error creating monthly sheets: {e}")


def prepare_update_requests(sheet_id, class_names, month_index):
    """Google Sheets更新用リクエストを準備"""
    if not class_names:
        print("Class names list is empty. Check data retrieved from Firebase.")
        return []

    requests = []

    # 教科名を追加
    requests.append(create_cell_update_request(month_index, 0, 0, "教科"))
    requests.extend(create_cell_update_request(month_index, i + 1, 0, name) for i, name in enumerate(class_names))

    # 日付を追加 (例: 月のカレンダー形式)
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(2025, month_index + 1, 1)
    end_row = 25

    for i in range(31):  # 最大31日分のデータ
        date = start_date + timedelta(days=i)
        if date.month != month_index + 1:  # 月が変わったら終了
            break
        weekday = date.weekday()
        date_string = f"{date.strftime('%m')}\n月\n{date.strftime('%d')}\n日\n⌢\n{japanese_weekdays[weekday]}\n⌣"
        requests.append(create_cell_update_request(month_index, 0, i + 1, date_string))

        # 土日を色付け
        if weekday in (5, 6):  # 土日
            color = {"red": 0.8, "green": 0.9, "blue": 1.0} if weekday == 5 else {"red": 1.0, "green": 0.8, "blue": 0.8}
            requests.append(create_conditional_formatting_request(
                month_index, 0, end_row, i + 1, i + 2, color,
                f'=ISNUMBER(SEARCH("{japanese_weekdays[weekday]}", INDIRECT(ADDRESS(1, COLUMN()))))'
            ))

    # 黒背景を設定
    requests.append(create_black_background_request(month_index, 25, 1000, 0, 1000))
    requests.append(create_black_background_request(month_index, 0, 1000, 32, 1000))

    return requests


def main():
    """メイン関数"""
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # Firebaseから必要なデータを取得
    sheet_id = get_firebase_data('Students/item/student_number/e19139/sheet_id')
    student_course_ids = get_firebase_data('Students/enrollment/student_number/e19139/course_id')
    courses = get_firebase_data('Courses/course_id')

    if not sheet_id:
        print("Sheet ID is missing or invalid.")
        return
    if not isinstance(student_course_ids, list) or not isinstance(courses, list):
        print("Invalid data retrieved from Firebase.")
        return

    courses_dict = {i: course for i, course in enumerate(courses) if course}

    class_names = [
        courses_dict[cid]['class_name'] for cid in student_course_ids
        if cid in courses_dict and 'class_name' in courses_dict[cid]
    ]

    create_monthly_sheets(sheet_id, sheets_service)

    # 各月のデータを更新
    for month_index in range(12):  # 1月～12月
        requests = prepare_update_requests(sheet_id, class_names, month_index)
        if not requests:
            print(f"No requests to update the sheet for month {month_index + 1}.")
            continue

        try:
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={'requests': requests}
            ).execute()
            print(f"Sheet for month {month_index + 1} updated successfully.")
        except Exception as e:
            print(f"Error updating sheet for month {month_index + 1}: {e}")


if __name__ == "__main__":
    main()
