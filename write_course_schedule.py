# 日付と曜日をスプレッドシートに記入するリクエストを準備
def add_dates_to_sheet(sheet_id, month, year, new_sheet_id):
    requests = []
    japanese_weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    
    # 月の初日と最終日を計算
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    
    # 日付列の開始位置
    column_index = 2
    current_date = start_date
    
    while current_date <= end_date:
        weekday = current_date.weekday()
        date_string = f"{current_date.strftime('%m/%d')} ({japanese_weekdays[weekday]})"
        
        # 日付を記入
        requests.append({
            "updateCells": {
                "rows": [{"values": [{"userEnteredValue": {"stringValue": date_string}}]}],
                "start": {"sheetId": new_sheet_id, "rowIndex": 0, "columnIndex": column_index},
                "fields": "userEnteredValue"
            }
        })
        
        # 背景色を設定 (土曜: 青, 日曜: 赤)
        if weekday == 5:  # 土曜日
            color = {"red": 0.8, "green": 0.9, "blue": 1.0}  # 青
        elif weekday == 6:  # 日曜日
            color = {"red": 1.0, "green": 0.8, "blue": 0.8}  # 赤
        else:
            color = None

        if color:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": new_sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": column_index,
                        "endColumnIndex": column_index + 1
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": color}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

        # 列を次に進める
        column_index += 1
        current_date += timedelta(days=1)
    
    return requests

# シート更新リクエストを準備 (修正後)
def prepare_update_requests(sheet_id, student_names, attendance_numbers, month, sheets_service, spreadsheet_id, year=2025):
    if not student_names:
        print("学生名リストが空です。")
        return []

    # シートを追加するリクエスト
    base_title = f"{year}-{str(month).zfill(2)}"
    add_sheet_request = {
        "addSheet": {"properties": {"title": base_title}}
    }
    requests = [add_sheet_request]

    # シート作成後にそのIDを取得
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

    # 学生データと日付をスプレッドシートに記入
    requests = []
    requests.append({"updateCells": {
        "rows": [{"values": [{"userEnteredValue": {"stringValue": "学生名"}}]}],
        "start": {"sheetId": new_sheet_id, "rowIndex": 0, "columnIndex": 1},
        "fields": "userEnteredValue"
    }})
    requests.append({"updateCells": {
        "rows": [{"values": [{"userEnteredValue": {"stringValue": "AN"}}]}],
        "start": {"sheetId": new_sheet_id, "rowIndex": 0, "columnIndex": 0},
        "fields": "userEnteredValue"
    }})

    for i, (name, number) in enumerate(zip(student_names, attendance_numbers)):
        requests.append({"updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": name}}]}],
            "start": {"sheetId": new_sheet_id, "rowIndex": i + 1, "columnIndex": 1},
            "fields": "userEnteredValue"
        }})
        requests.append({"updateCells": {
            "rows": [{"values": [{"userEnteredValue": {"stringValue": str(number)}}]}],
            "start": {"sheetId": new_sheet_id, "rowIndex": i + 1, "columnIndex": 0},
            "fields": "userEnteredValue"
        }})

    # 日付を追加
    date_requests = add_dates_to_sheet(sheet_id, month, year, new_sheet_id)
    requests.extend(date_requests)

    return requests

# メイン処理 (変更なし)
def main():
    initialize_firebase()
    sheets_service = get_google_sheets_service()

    # 各コースに対応する処理を実行
    courses = get_firebase_data("Courses/course_id")
    if not courses or not isinstance(courses, list):
        print("Courses データが見つかりません。")
        return

    for course_id in range(1, len(courses)):  # 1から開始
        print(f"Processing Course ID: {course_id}")
        sheet_id = get_sheet_id(course_id)
        if not sheet_id:
            continue

        student_names, attendance_numbers = get_students_by_course(course_id)
        if not student_names:
            print(f"Course ID {course_id} の学生リストが空です。")
            continue

        for month in range(1, 13):
            print(f"Processing month: {month} for Course ID: {course_id}")
            requests = prepare_update_requests(sheet_id, student_names, attendance_numbers, month, sheets_service, sheet_id)
            if not requests:
                print(f"月 {month} のシートを更新するリクエストがありません。")
                continue

            execute_with_retry(
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={'requests': requests}
                )
            )
            print(f"月 {month} のシートを正常に更新しました。")

if __name__ == "__main__":
    main()
