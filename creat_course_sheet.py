import firebase_admin
from firebase_admin import credentials, db
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Initialize Firebase app if not already initialized
try:
    firebase_admin.get_app()
except ValueError:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Define the scopes for Google Sheets and Drive API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = '/tmp/gcp_service_account.json'

# Get credentials from the service account file
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

def create_spreadsheets_for_courses():
    try:
        # Retrieve all courses from the Firebase database
        courses_ref = db.reference('Courses/course_id')
        all_courses = courses_ref.get()

        if not all_courses:
            print("No course data found in Firebase.")
            return

        for course_index, course_data in enumerate(all_courses):
            if not course_data:
                continue

            course_name = course_data.get('course_name', "Unnamed Course")

            # Create a new spreadsheet
            with build('sheets', 'v4', credentials=creds) as sheets_service:
                spreadsheet = {
                    'properties': {'title': course_name}
                }
                spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
                sheet_id = spreadsheet.get('spreadsheetId')
                print(f'Spreadsheet ID for {course_name}: {sheet_id}')

            # Set permissions for the spreadsheet
            with build('drive', 'v3', credentials=creds) as drive_service:
                permissions = [{'type': 'user', 'role': 'writer', 'emailAddress': 'naru.ibuki020301@gmail.com'}]
                batch = drive_service.new_batch_http_request()

                for permission in permissions:
                    batch.add(drive_service.permissions().create(
                        fileId=sheet_id,
                        body=permission,
                        fields='id'
                    ))
                batch.execute()

            # Save the spreadsheet ID to Firebase
            course_ref = db.reference(f'Courses/course_id/{course_index}')
            course_ref.update({'course_sheet_id': sheet_id})

    except HttpError as error:
        print(f'API error occurred: {error}')
    except Exception as e:
        print(f'An error occurred: {e}')

create_spreadsheets_for_courses()
