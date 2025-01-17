import firebase_admin
from firebase_admin import credentials, db

# Firebase初期化
if not firebase_admin._apps:
    cred = credentials.Certificate('/tmp/firebase_service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-51ebc-default-rtdb.firebaseio.com/'
    })

# データベース参照
ref = db.reference()

# CoursesとStudentsのデータ取得
courses_data = ref.child('Courses/course_id').get()
students_data = ref.child('Students/student_info/student_index').get()

# Classデータのための辞書
class_data = {}

# Coursesの処理
if courses_data:
    for index, course_info in enumerate(courses_data):
        if course_info is None:
            continue
        class_name = course_info.get('class_name')
        if class_name:
            # class_nameごとにcourse_idを格納
            if class_name not in class_data:
                class_data[class_name] = {
                    'course_ids': [],
                    'student_indices': []
                }
            class_data[class_name]['course_ids'].append(index)

# Studentsの処理
if students_data:
    for student_index, student_info in students_data.items():
        if len(student_index) >= 2:  # student_indexの最初の2文字を確認
            class_index = student_index[:2]
            # class_indexがclass_dataに存在する場合、student_indexを格納
            if class_index in class_data:
                class_data[class_index]['student_indices'].append(student_index)

# データベースにClassデータを格納
class_ref = ref.child('Class')
for class_index, data in class_data.items():
    # course_idを保存
    for i, course_id in enumerate(data['course_ids'], start=1):
        class_ref.child(f'{class_index}/course_id/{i}').set(course_id)
    
    # student_indexを保存
    for student_index in data['student_indices']:
        class_ref.child(f'{class_index}/student_index/{student_index}').set(student_index)

print("データの処理と格納が完了しました。")
