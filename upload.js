const fs = require('fs');
const admin = require('firebase-admin');

// Firebase Admin SDK の初期化
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://test-51ebc-default-rtdb.firebaseio.com/'
});

const db = admin.database();

// data.json ファイルの存在確認
const dataFilePath = './data.json';
if (!fs.existsSync(dataFilePath)) {
  throw new Error(`JSON ファイルが見つかりません: ${dataFilePath}`);
}

// data.json を読み込み・解析
const formData = JSON.parse(fs.readFileSync(dataFilePath, 'utf8'));

// 必須フィールドのバリデーション
const requiredFields = ['studentId', 'name', 'attendanceNumber', 'department', 'year', 'courseId'];
for (const field of requiredFields) {
  if (!formData[field]) {
    throw new Error(`必須フィールドが不足しています: ${field}`);
  }
}

// Firebase の保存パスを構築
const studentNumber = formData.studentId;
const refPath = `Students/student_number/${studentNumber}`;

console.log(`Firebase にデータを保存中: パス = ${refPath}`);

// データを Firebase に保存
const ref = db.ref(refPath);
ref.set(formData)
  .then(() => console.log(`データが正常にFirebaseへ保存されました: パス = ${refPath}`))
  .catch(error => {
    console.error('Firebase へのデータ保存中にエラーが発生しました:', error);
    process.exit(1);
  });
