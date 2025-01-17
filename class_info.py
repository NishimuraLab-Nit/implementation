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
            if class_name not in class_data:
                class_data[class_name] = {
                    'course_ids': [],
                    'student_indices': []
                }
            class_data[class_name]['course_ids'].append(str(index))

# Studentsの処理
if students_data:
    for student_index, student_info in students_data.items():
        if len(student_index) >= 2:  # student_indexの最初の2文字を確認
            class_index = student_index[:2]
            if class_index not in class_data:
                class_data[class_index] = {
                    'course_ids': [],
                    'student_indices': []
                }
            class_data[class_index]['student_indices'].append(student_index)

# データベースにClassデータを格納
class_ref = ref.child('Class')
for class_index, data in class_data.items():
    # course_idをカンマ区切りの文字列に変換して保存
    course_ids_str = ', '.join(data['course_ids'])
    class_ref.child(f'{class_index}/course_id').set(course_ids_str)
    
    # student_indexをカンマ区切りの文字列に変換して保存
    student_indices_str = ', '.join(data['student_indices'])
    class_ref.child(f'{class_index}/student_index').set(student_indices_str)


# class_indexの情報を設定
class_index_data = ref.child('Class/class_index').get()

if class_index_data:
    # class_indexに存在するクラスも考慮して格納
    for class_index, class_info in class_index_data.items():
        if class_index not in class_data:
            # クラスが存在しない場合、空のエントリを作成
            class_ref.child(f'class_index/{class_index}/course_id').set('')
            class_ref.child(f'class_index/{class_index}/student_index').set('')
        # class_indexの詳細を保存
        class_ref.child(f'class_index/{class_index}').set(class_info)
else:
    print("Warning: class_indexデータが存在しません。")

print("データの処理と格納が完了しました。")
