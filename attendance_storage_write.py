import firebase_admin
from firebase_admin import credentials, db
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
import re

def initialize_firebase():
    """
    Firebase Admin SDK の初期化を行います。
    すでに初期化されている場合は再初期化を行いません。
    """
    if not firebase_admin._apps:
        cred = credentials.Certificate("/tmp/firebase_service_account.json")
        firebase_admin.initialize_app(
            cred,
            {"databaseURL": "https://test-51ebc-default-rtdb.firebaseio.com/"},
        )

def create_google_services():
    """
    Google Sheets および Drive API のクライアントを作成して返します。
    """
    creds = service_account.Credentials.from_service_account_file(
        "/tmp/gcp_service_account.json",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    sheets_service = build("sheets", "v4", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)
    return sheets_service, drive_service

def get_attendance_spreadsheet_id():
    """
    Firebase上の Students/attendance/attendance_sheet_id から
    スプレッドシートIDを取得して返す。
    """
    sheet_id_ref = db.reference("Students/attendance/attendance_sheet_id")
    sheet_id = sheet_id_ref.get()
    if not sheet_id:
        raise ValueError("attendance_sheet_id がFirebase上に存在しません。")
    return sheet_id

def add_new_sheet(sheets_service, spreadsheet_id, sheet_name):
    """
    既存のスプレッドシート(spreadsheet_id)に、
    新しいシート(sheet_name)を追加し、行数2000、列数25に設定する。

    すでに同名シートがある場合はエラーとなるので、
    名前が重複しない前提で利用する。
    """
    requests = [
        {
            "addSheet": {
                "properties": {
                    "title": sheet_name,
                    "gridProperties": {
                        "rowCount": 2000,
                        "columnCount": 25
                    }
                }
            }
        }
    ]
    body = {"requests": requests}
    response = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()
    return response

def get_sheet_id_by_name(sheets_service, spreadsheet_id, sheet_name):
    """
    シート名から sheetId (数値) を取得。
    存在しない場合は None を返す。
    """
    spreadsheet = sheets_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id
    ).execute()
    for sheet in spreadsheet.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_name:
            return props.get("sheetId")
    return None

def write_header_row(sheets_service, spreadsheet_id, sheet_name):
    """
    規定のカラム名を1行目に書き込みます。
    A列: student_id
    以下、最大4ペア(entry1/exit1, entry2/exit2, ...)を考慮。
    """
    header = ["student_id"]
    # entry/exit 1ペアあたり6列: [entryX, read_datetime, serial_number, exitX, read_datetime, serial_number]
    for i in range(1, 5):
        header.append(f"entry{i}")
        header.append("read_datetime")
        header.append("serial_number")
        header.append(f"exit{i}")
        header.append("read_datetime")
        header.append("serial_number")

    data = [
        {
            "range": f"{sheet_name}!A1:Y1",
            "values": [header]
        }
    ]
    body = {
        "valueInputOption": "RAW",
        "data": data
    }
    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()

def apply_table_formatting(sheets_service, spreadsheet_id, sheet_name):
    """
    1) sheet_name に対し、A1~Y2000 の範囲を Named Range として作成 ("MyTable" 等)
    2) その範囲に罫線を引いてテーブル風に書式設定
    """
    # シートID (整数) を取得
    sheet_id = get_sheet_id_by_name(sheets_service, spreadsheet_id, sheet_name)
    if sheet_id is None:
        return  # シートが見つからなければなにもしない

    # Named Rangeを追加するリクエスト (A1:Y2000)
    named_range_request = {
        "addNamedRange": {
            "namedRange": {
                "name": "MyTable",  # 好きな名前でOK
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,   # A1の行=0
                    "endRowIndex": 2000,  # 2000行目まで
                    "startColumnIndex": 0,  # A列=0
                    "endColumnIndex": 25    # Y列(25列)
                }
            }
        }
    }

    # 罫線を引くリクエスト (外枠と内枠)
    # 同じ範囲(A1:Y2000)に対し top/bottom/left/right/innerHorizontal/innerVertical を設定
    border_style = {
        "style": "SOLID",
        "width": 1,
        "color": {"red": 0, "green": 0, "blue": 0}  # 黒線
    }
    borders_request = {
        "updateBorders": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 2000,
                "startColumnIndex": 0,
                "endColumnIndex": 25
            },
            "top": border_style,
            "bottom": border_style,
            "left": border_style,
            "right": border_style,
            "innerHorizontal": border_style,
            "innerVertical": border_style
        }
    }

    requests = [named_range_request, borders_request]

    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()

def export_attendance_data():
    """
    1) Firebaseから attendance_sheet_id を取得し、
       その既存スプレッドシートに当日の日付シートを追加
    2) Students/attendance/student_id/{student_id} 以下の entryX / exitX データを取得
    3) 取得した情報をシートに書き込み、Firebase から entryX / exitX を削除
    4) 追加したシートに「テーブル風の書式付け」を適用
    """
    # Firebase初期化
    initialize_firebase()
    # Google APIサービス生成
    sheets_service, _ = create_google_services()

    # 既存のスプレッドシートIDを Firebase から取得
    spreadsheet_id = get_attendance_spreadsheet_id()

    # 実行日をシート名にする
    today_str = datetime.now().strftime("%Y-%m-%d")

    # 新しいシートを追加 (同名シート存在しない前提)
    add_new_sheet(sheets_service, spreadsheet_id, today_str)

    # ヘッダー行を記入
    write_header_row(sheets_service, spreadsheet_id, today_str)

    # Firebaseから出席情報を取得
    attendance_ref = db.reference("Students/attendance/student_id")
    attendance_data = attendance_ref.get() or {}

    # 書き込み用データ
    rows_to_write = []
    current_row = 2  # 2行目から書き込み

    # student_id ごとに処理
    for student_id, actions_dict in attendance_data.items():
        if not isinstance(actions_dict, dict):
            continue

        # 25列分の空欄を用意
        row_data = [""] * 25
        # A列に student_id
        row_data[0] = student_id

        # entryX/exitX を最大4ペアぶん一時保管
        pairs = {
            1: {"entry": None, "exit": None},
            2: {"entry": None, "exit": None},
            3: {"entry": None, "exit": None},
            4: {"entry": None, "exit": None},
        }

        # キーが entry1, exit1, entry2, exit2 ... の形を想定
        for key, val in actions_dict.items():
            if not isinstance(val, dict):
                continue
            if key.startswith("entry") or key.startswith("exit"):
                m = re.match(r"(entry|exit)(\d+)", key)
                if m:
                    action_type = m.group(1)  # "entry" or "exit"
                    action_num_str = m.group(2)
                    try:
                        action_num = int(action_num_str)
                        if action_num in pairs:
                            pairs[action_num][action_type] = val
                    except ValueError:
                        pass

        # ペアごとに row_data に書き込み
        for i in range(1, 5):
            col_start = 1 + (i - 1) * 6  # 1ペア6列、B列(インデックス1)から
            entry_info = pairs[i]["entry"]
            exit_info = pairs[i]["exit"]

            if entry_info:
                row_data[col_start] = f"entry{i}"
                row_data[col_start+1] = entry_info.get("read_datetime", "")
                row_data[col_start+2] = entry_info.get("serial_number", "")

            if exit_info:
                row_data[col_start+3] = f"exit{i}"
                row_data[col_start+4] = exit_info.get("read_datetime", "")
                row_data[col_start+5] = exit_info.get("serial_number", "")

        rows_to_write.append(row_data)

        # Firebase から entry/exit を削除
        for key in list(actions_dict.keys()):
            if key.startswith("entry") or key.startswith("exit"):
                attendance_ref.child(student_id).child(key).delete()
                attendance_ref.child(course_id).delete()

        current_row += 1

    # スプレッドシートに一括書き込み
    if rows_to_write:
        # A2～Y(行数)まで一気に書き込む
        range_notation = f"{today_str}!A2:Y{1 + len(rows_to_write)}"
        body = {
            "valueInputOption": "RAW",
            "data": [
                {
                    "range": range_notation,
                    "values": rows_to_write
                }
            ]
        }
        sheets_service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()

    # 最後にテーブル書式を適用 (罫線＆Named Range)
    apply_table_formatting(sheets_service, spreadsheet_id, today_str)

    print("出席データのエクスポートが完了し、テーブル型に整形しました。")

def main():
    try:
        export_attendance_data()
    except HttpError as e:
        print(f"HTTPエラーが発生しました: {e}")
    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    main()
