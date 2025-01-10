from firebase_admin import credentials, initialize_app, db
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta


def initialize_firebase():
    """Firebaseの初期化"""
    try:
        firebase_cred = credentials.Certificate("firebase-adminsdk.json")
        initialize_app(firebase_cred, {
            'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
        })
        print("Firebaseの初期化に成功しました。")
    except Exception as e:
        print(f"Firebaseの初期化中にエラーが発生しました: {e}")
        raise


def get_google_sheets_service():
    """Google Sheets APIのサービスを取得"""
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        google_creds = Credentials.from_service_account_file("google-credentials.json", scopes=scopes)
        return build('sheets', 'v4', credentials=google_creds)
    except Exception as e:
        print(f"Google Sheets APIの初期化中にエラーが発生しました: {e}")
        raise


def get_firebase_data(ref_path):
    """Firebaseからデータを取得"""
    try:
        data = db.reference(ref_path).get()
        print(f"Firebaseからデータを取得しました ({ref_path}): {data}")
        return data
    except Exception as e:
        print(f"Firebaseからデータを取得中にエラーが発生しました ({ref_path}): {e}")
        return None


def validate_firebase_data(sheet_id, student_course_ids, courses):
    """Firebaseデータの検証"""
    if not isinstance(sheet_id, str) or not sheet_id.strip():
        raise ValueError("シートIDが無効、または存在しません。")

    if not isinstance(student_course_ids, list) or not student_course_ids:
        raise ValueError("学生のコースIDリストが無効、または存在しません。")

    if not isinstance(courses, list) or not all(isinstance(course, dict) and 'class_name' in course for course in courses):
        raise ValueError("コースデータが無効、または不完全です。")


def get_existing_sheet_titles(sheets_service, sheet_id):
    """既存のシートタイトルを取得"""
    try:
        response = sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheets = response.get('sheets', [])
        titles = [sheet['properties']['title'] for sheet in sheets]
        return titles
    except Exception as e:
        print(f"既存のシートタイトルを取得中にエラーが発生しました: {e}")
        return []


def prepare_monthly_sheets(sheet_id, sheets_service):
    """1月～12月のシートを作成"""
    months = ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"]
    existing_titles = get_existing_sheet_titles(sheets_service, sheet_id)

    requests = [{"addSheet": {"properties": {"title": month}}} for month in months if month not in existing_titles]

    if not requests:
        print("すべての月のシートが既に存在しています。新しいシートは追加されませんでした。")
        return

    try:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": requests}
        ).execute()
        print("月ごとのシートが正常に作成されました。")
    except Exception as e:
        print(f"月ごとのシートを作成中にエラーが発生しました: {e}")


def create_cell_update_request(sheet_id, row_index, column_index, value):
    """Google Sheetsのセル更新リクエストを作成"""
    return {
        "updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": value}}]}],
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
            "fields": "userEnteredValue"
        }
    }


def prepare_update_requests(sheet_id, class_names, month_index):
    """Google Sheets更新用リクエストを準備"""
    if not class_names:
        print("クラス名のリストが空です。Firebaseから取得したデータを確認してください。")
        return []

    requests = []

    # 教科名を追加
    requests.append(create_cell_update_request(sheet_id, 0, 0, "教科"))
    requests.extend(create_cell_update_request(sheet_id, i + 1, 0, name) for i, name in enumerate(class_names))

    # 日付を追加
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    start_date = datetime(2025, month_index + 1, 1)

    for i in range(31):  # 最大31日分のデータ
        date = start_date + timedelta(days=i)
        if date.month != month_index + 1:  # 月が変わったら終了
            break
        weekday = date.weekday()
        date_string = f"{date.strftime('%m/%d')} ({japanese_weekdays[weekday]})"
        requests.append(create_cell_update_request(sheet_id, 0, i + 1, date_string))

    return requests


def main():
    """メイン関数"""
    try:
        initialize_firebase()
        sheets_service = get_google_sheets_service()

        # Firebaseから必要なデータを取得
        sheet_id = get_firebase_data('Students/student_info/student_index/E534/sheet_id')
        student_course_ids = get_firebase_data('Students/enrollment/student_index/E534/course_id')
        courses = get_firebase_data('Courses/course_id')

        # データの検証
        validate_firebase_data(sheet_id, student_course_ids, courses)

        # コース情報を辞書化
        courses_dict = {str(i): course for i, course in enumerate(courses) if course}

        # クラス名リストを作成
        class_names = [
            courses_dict[cid]['class_name'] for cid in student_course_ids
            if cid in courses_dict and 'class_name' in courses_dict[cid]
        ]

        # 1月～12月のシート作成
        prepare_monthly_sheets(sheet_id, sheets_service)

        # 各月のデータを更新
        for month_index in range(12):  # 1月～12月
            requests = prepare_update_requests(sheet_id, class_names, month_index)
            if not requests:
                print(f"{month_index + 1}月のシートを更新するリクエストがありません。")
                continue

            try:
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={'requests': requests}
                ).execute()
                print(f"{month_index + 1}月のシートが正常に更新されました。")
            except Exception as e:
                print(f"{month_index + 1}月のシートを更新中にエラーが発生しました: {e}")

    except Exception as e:
        print(f"予期しないエラーが発生しました: {e}")


if __name__ == "__main__":
    main()
