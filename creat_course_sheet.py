import firebase_admin
from firebase_admin import credentials, db
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Initialize the Firebase app if not already initialized
if not firebase_admin._apps:
    # Specify service account credentials
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    # Initialize the Firebase app
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# Define the scopes for Google Sheets and Drive API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
# Path to the service account file
SERVICE_ACCOUNT_FILE = '/tmp/gcp_service_account.json'

# Get credentials from the service account file
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

# Create service clients for Google Sheets and Drive
sheets_service = build('sheets', 'v4', credentials=creds)
drive_service = build('drive', 'v3', credentials=creds)

def create_spreadsheets_for_courses():
    try:
        # Retrieve all courses from the Firebase database
        courses_ref = db.reference('Courses/course_id')
        all_courses = courses_ref.get()

        if all_courses is None:
            raise ValueError("No course data found in Firebase.")

        for course_data in all_courses:
            if course_data is None:
                continue

            class_name = course_data.get('class_name', "Unnamed Course")

            # Create a new spreadsheet
            spreadsheet = {
                'properties': {'title': class_name}
            }
            spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
            sheet_id = spreadsheet.get('spreadsheetId')
            print(f'Spreadsheet ID for {class_name}: {sheet_id}')

            # Set permissions for the spreadsheet
            permissions = [
                {'type': 'user', 'role': 'writer', 'emailAddress': 'naru.ibuki020301@gmail.com'}
            ]

            # Add permissions in batch
            batch = drive_service.new_batch_http_request()
            for permission in permissions:
                batch.add(drive_service.permissions().create(
                    fileId=sheet_id,
                    body=permission,
                    fields='id'
                ))
            batch.execute()

            # Save the spreadsheet ID to Firebase
            course_index = all_courses.index(course_data)
            item_ref = db.reference(f'Courses/course_id/{course_index}')
            item_ref.update({'course_sheet_id': sheet_id})

    except HttpError as error:
        print(f'API error occurred: {error}')
    except ValueError as e:
        print(e)

create_spreadsheets_for_courses()
