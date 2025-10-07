from flask import Flask, request, jsonify, render_template_string, send_file
import os
import sqlite3
from datetime import datetime
import pandas as pd
import requests
from werkzeug.utils import secure_filename
import io

app = Flask(__name__)

# Настройки
UPLOAD_FOLDER = 'uploads'
DB_PATH = 'checks.db'
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

# Создаем папку для загрузок
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Настройки Bankiros (ЗАМЕНИ НА СВОИ!)
BANKIROS_TOKEN = "kOjFk444n6txgf6DFps"  # ВАЖНО: Замени на реальный токен!
BANKIROS_URL = "http://api.dev.mainfin.ru"  # Тестовый сервер
POSTBACK_URL = "https://bankiros-webhook.onrender.com/bankiros/postback"
OFFER_IDS = [459]  # ID МФО для проверки

# Инициализация базы данных
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            check_id INTEGER UNIQUE,
            phone TEXT,
            employer_inn TEXT,
            offer_id INTEGER,
            status TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

init_db()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# HTML шаблон веб-интерфейса
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bankiros - Проверка дублей клиентов</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 900px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            padding: 40px;
        }
        
        h1 {
            color: #2C3E50;
            text-align: center;
            margin-bottom: 10px;
            font-size: 2.5em;
        }
        
        .subtitle {
            text-align: center;
            color: #7f8c8d;
            margin-bottom: 40px;
            font-size: 1.1em;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }
        
        .stat-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 25px;
            border-radius: 15px;
            text-align: center;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        
        .stat-card h3 {
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        
        .stat-card p {
            font-size: 1em;
            opacity: 0.9;
        }
        
        .section {
            background: #f8f9fa;
            padding: 30px;
            border-radius: 15px;
            margin-bottom: 30px;
        }
        
        .section h2 {
            color: #2C3E50;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .file-upload {
            border: 3px dashed #667eea;
            border-radius: 15px;
            padding: 40px;
            text-align: center;
            cursor: pointer;
            transition: all 0.3s;
            background: white;
        }
        
        .file-upload:hover {
            border-color: #764ba2;
            background: #f0f0ff;
        }
        
        .file-upload input[type="file"] {
            display: none;
        }
        
        .btn {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 15px 40px;
            font-size: 1.1em;
            border-radius: 10px;
            cursor: pointer;
            transition: all 0.3s;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
            font-weight: bold;
        }
        
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 7px 20px rgba(0,0,0,0.3);
        }
        
        .btn:disabled {
            background: #95a5a6;
            cursor: not-allowed;
            transform: none;
        }
        
        .btn-success {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }
        
        .btn-danger {
            background: linear-gradient(135deg, #ee0979 0%, #ff6a00 100%);
        }
        
        .progress-bar {
            width: 100%;
            height: 30px;
            background: #ecf0f1;
            border-radius: 15px;
            overflow: hidden;
            margin: 20px 0;
            display: none;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            transition: width 0.3s;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
        }
        
        .message {
            padding: 15px 20px;
            border-radius: 10px;
            margin: 15px 0;
            display: none;
        }
        
        .message.success {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        
        .message.error {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        
        .message.info {
            background: #d1ecf1;
            color: #0c5460;
            border: 1px solid #bee5eb;
        }
        
        .instructions {
            background: #fff3cd;
            border: 1px solid #ffc107;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 30px;
        }
        
        .instructions h3 {
            color: #856404;
            margin-bottom: 15px;
        }
        
        .instructions ol {
            margin-left: 20px;
        }
        
        .instructions li {
            margin: 8px 0;
            color: #856404;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔍 Bankiros - Проверка дублей</h1>
        <p class="subtitle">Система массовой проверки клиентов</p>
        
        <!-- Статистика -->
        <div class="stats-grid" id="stats">
            <div class="stat-card">
                <h3 id="total">0</h3>
                <p>Всего проверок</p>
            </div>
            <div class="stat-card">
                <h3 id="duplicates">0</h3>
                <p>Дублей найдено</p>
            </div>
            <div class="stat-card">
                <h3 id="new-clients">0</h3>
                <p>Новых клиентов</p>
            </div>
            <div class="stat-card">
                <h3 id="pending">0</h3>
                <p>В обработке</p>
            </div>
        </div>
        
        <!-- Инструкции -->
        <div class="instructions">
            <h3>📋 Как пользоваться:</h3>
            <ol>
                <li>Подготовьте Excel/CSV файл с колонками <strong>"телефон"</strong> и <strong>"ИНН"</strong></li>
                <li>Загрузите файл через форму ниже</li>
                <li>Нажмите "Отправить проверки" и дождитесь завершения</li>
                <li>Через несколько часов скачайте результаты</li>
            </ol>
        </div>
        
        <!-- Раздел 1: Загрузка файла -->
        <div class="section">
            <h2>📤 Шаг 1: Загрузить файл с клиентами</h2>
            <div class="file-upload" onclick="document.getElementById('fileInput').click()">
                <p style="font-size: 3em; margin-bottom: 10px;">📁</p>
                <p style="font-size: 1.2em; color: #667eea; font-weight: bold;">Нажмите для выбора файла</p>
                <p style="color: #7f8c8d; margin-top: 10px;">Excel (.xlsx, .xls) или CSV файлы</p>
                <input type="file" id="fileInput" accept=".xlsx,.xls,.csv" onchange="uploadFile()">
            </div>
            <div id="uploadMessage" class="message"></div>
        </div>
        
        <!-- Раздел 2: Отправка проверок -->
        <div class="section">
            <h2>🚀 Шаг 2: Отправить проверки</h2>
            <p style="margin-bottom: 20px; color: #7f8c8d;">После загрузки файла нажмите кнопку для начала проверки</p>
            <button class="btn" id="sendBtn" onclick="sendChecks()" disabled>
                📨 Отправить проверки
            </button>
            <div class="progress-bar" id="progressBar">
                <div class="progress-fill" id="progressFill">0%</div>
            </div>
            <div id="sendMessage" class="message"></div>
        </div>
        
        <!-- Раздел 3: Скачивание результатов -->
        <div class="section">
            <h2>📥 Шаг 3: Скачать результаты</h2>
            <p style="margin-bottom: 20px; color: #7f8c8d;">Скачайте файл с результатами проверок</p>
            <button class="btn btn-success" onclick="downloadResults()">
                💾 Скачать результаты (Excel)
            </button>
            <div id="downloadMessage" class="message"></div>
        </div>
        
        <!-- Раздел 4: Очистка базы -->
        <div class="section">
            <h2>🗑️ Дополнительно: Очистить базу данных</h2>
            <p style="margin-bottom: 20px; color: #e74c3c;">⚠️ Внимание! Это удалит ВСЕ данные проверок</p>
            <button class="btn btn-danger" onclick="clearDatabase()">
                🗑️ Очистить все данные
            </button>
            <div id="clearMessage" class="message"></div>
        </div>
    </div>
    
    <script>
        // Загрузка статистики при открытии страницы
        loadStats();
        
        // Обновление статистики каждые 10 секунд
        setInterval(loadStats, 10000);
        
        function loadStats() {
            fetch('/api/stats')
                .then(response => response.json())
                .then(data => {
                    document.getElementById('total').textContent = data.total_checks;
                    document.getElementById('duplicates').textContent = data.duplicates;
                    document.getElementById('new-clients').textContent = data.not_duplicates;
                    document.getElementById('pending').textContent = data.pending;
                });
        }
        
        function uploadFile() {
            const fileInput = document.getElementById('fileInput');
            const file = fileInput.files[0];
            
            if (!file) return;
            
            const formData = new FormData();
            formData.append('file', file);
            
            showMessage('uploadMessage', 'info', '⏳ Загружаем файл...');
            
            fetch('/api/upload', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showMessage('uploadMessage', 'success', `✅ ${data.message} (${data.records} записей)`);
                    document.getElementById('sendBtn').disabled = false;
                } else {
                    showMessage('uploadMessage', 'error', `❌ ${data.message}`);
                }
            })
            .catch(error => {
                showMessage('uploadMessage', 'error', '❌ Ошибка загрузки файла');
            });
        }
        
        function sendChecks() {
            if (!confirm('Начать массовую проверку?\\n\\nЭто может занять продолжительное время.')) {
                return;
            }
            
            document.getElementById('sendBtn').disabled = true;
            document.getElementById('progressBar').style.display = 'block';
            showMessage('sendMessage', 'info', '⏳ Отправляем проверки...');
            
            fetch('/api/send-checks', {
                method: 'POST'
            })
            .then(response => response.json())
            .then(data => {
                document.getElementById('progressBar').style.display = 'none';
                
                if (data.success) {
                    showMessage('sendMessage', 'success', 
                        `✅ Проверки отправлены!\\n\\nОтправлено: ${data.sent}\\nОшибок: ${data.errors}\\n\\nРезультаты придут через несколько часов.`
                    );
                    loadStats();
                } else {
                    showMessage('sendMessage', 'error', `❌ ${data.message}`);
                }
                
                document.getElementById('sendBtn').disabled = false;
            })
            .catch(error => {
                document.getElementById('progressBar').style.display = 'none';
                showMessage('sendMessage', 'error', '❌ Ошибка отправки проверок');
                document.getElementById('sendBtn').disabled = false;
            });
        }
        
        function downloadResults() {
            showMessage('downloadMessage', 'info', '⏳ Формируем файл...');
            
            window.location.href = '/api/download-results';
            
            setTimeout(() => {
                showMessage('downloadMessage', 'success', '✅ Файл скачан!');
            }, 1000);
        }
        
        function clearDatabase() {
            if (!confirm('⚠️ ВЫ УВЕРЕНЫ?\\n\\nЭто удалит ВСЕ данные проверок без возможности восстановления!')) {
                return;
            }
            
            if (!confirm('Последнее предупреждение!\\n\\nДействительно удалить ВСЕ данные?')) {
                return;
            }
            
            fetch('/api/clear-database', {
                method: 'POST'
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showMessage('clearMessage', 'success', '✅ База данных очищена');
                    loadStats();
                } else {
                    showMessage('clearMessage', 'error', `❌ ${data.message}`);
                }
            });
        }
        
        function showMessage(elementId, type, text) {
            const element = document.getElementById(elementId);
            element.className = `message ${type}`;
            element.textContent = text;
            element.style.display = 'block';
            
            setTimeout(() => {
                element.style.display = 'none';
            }, 10000);
        }
    </script>
</body>
</html>
'''

@app.route('/')
def home():
    """Главная страница с веб-интерфейсом"""
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/stats')
def api_stats():
    """API: Получить статистику"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM checks')
    total = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM checks WHERE status = "duplicate"')
    duplicates = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM checks WHERE status = "not_duplicate"')
    not_duplicates = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM checks WHERE status = "pending" OR status IS NULL')
    pending = cursor.fetchone()[0]
    
    conn.close()
    
    return jsonify({
        "total_checks": total,
        "duplicates": duplicates,
        "not_duplicates": not_duplicates,
        "pending": pending
    })

@app.route('/api/upload', methods=['POST'])
def api_upload():
    """API: Загрузить файл"""
    if 'file' not in request.files:
        return jsonify({"success": False, "message": "Файл не выбран"})
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"success": False, "message": "Файл не выбран"})
    
    if not allowed_file(file.filename):
        return jsonify({"success": False, "message": "Недопустимый формат файла"})
    
    try:
        filename = secure_filename(file.filename)
        filepath = os.path.join(UPLOAD_FOLDER, 'clients.' + filename.rsplit('.', 1)[1])
        file.save(filepath)
        
        # Проверяем файл
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
        
        if 'телефон' not in df.columns or 'ИНН' not in df.columns:
            return jsonify({
                "success": False, 
                "message": "В файле должны быть столбцы 'телефон' и 'ИНН'"
            })
        
        return jsonify({
            "success": True,
            "message": "Файл успешно загружен",
            "records": len(df)
        })
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Ошибка: {str(e)}"})

@app.route('/api/send-checks', methods=['POST'])
def api_send_checks():
    """API: Отправить проверки"""
    try:
        # Находим загруженный файл
        files = os.listdir(UPLOAD_FOLDER)
        if not files:
            return jsonify({"success": False, "message": "Файл не найден. Сначала загрузите файл."})
        
        filepath = os.path.join(UPLOAD_FOLDER, files[0])
        
        # Читаем файл
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
        
        sent = 0
        errors = 0
        
        # Отправляем проверки
        for index, row in df.iterrows():
            phone = str(row['телефон'])
            inn = str(row['ИНН'])
            
            result = send_check_to_bankiros(phone, inn)
            
            if result['success']:
                sent += 1
            else:
                errors += 1
        
        return jsonify({
            "success": True,
            "sent": sent,
            "errors": errors
        })
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Ошибка: {str(e)}"})

def send_check_to_bankiros(phone, employer_inn):
    """Отправка проверки в Bankiros API"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Token {BANKIROS_TOKEN}"
    }
    
    payload = {
        "offerIds": OFFER_IDS,
        "formData": {
            "phone": phone,
            "is_accepted": "1",
            "employer_inn": employer_inn
        },
        "postbackUrl": POSTBACK_URL
    }
    
    try:
        response = requests.post(
            f"{BANKIROS_URL}/offers_partners_v1/partner-check-phone/import",
            headers=headers,
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            check_id = result.get('id')
            
            # Сохраняем в БД
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO checks (check_id, phone, employer_inn, status, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (check_id, phone, employer_inn, 'pending', datetime.now()))
            conn.commit()
            conn.close()
            
            return {"success": True, "check_id": check_id}
        else:
            return {"success": False, "error": f"HTTP {response.status_code}"}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.route('/api/download-results')
def api_download_results():
    """API: Скачать результаты"""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query('''
            SELECT phone as "Телефон", 
                   employer_inn as "ИНН",
                   status as "Статус проверки",
                   offer_id as "ID МФО",
                   updated_at as "Дата проверки"
            FROM checks
            ORDER BY updated_at DESC
        ''', conn)
        conn.close()
        
        # Создаем Excel в памяти
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Результаты')
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'bankiros_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Ошибка: {str(e)}"})

@app.route('/api/clear-database', methods=['POST'])
def api_clear_database():
    """API: Очистить базу данных"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM checks')
        conn.commit()
        conn.close()
        
        # Удаляем загруженные файлы
        for file in os.listdir(UPLOAD_FOLDER):
            os.remove(os.path.join(UPLOAD_FOLDER, file))
        
        return jsonify({"success": True})
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Ошибка: {str(e)}"})

@app.route('/bankiros/postback', methods=['POST'])
def receive_postback():
    """Endpoint для приема постбэков от Bankiros"""
    try:
        data = request.get_json()
        
        offer_id = data.get('offerId')
        status = data.get('status')
        check_id = data.get('id')
        
        print(f"Получен постбэк: ID={check_id}, OfferID={offer_id}, Status={status}")
        
        # Обновляем статус в БД
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE checks 
            SET offer_id = ?, status = ?, updated_at = ?
            WHERE check_id = ?
        ''', (offer_id, status, datetime.now(), check_id))
        conn.commit()
        conn.close()
        
        return jsonify({"success": True, "received": True}), 200
        
    except Exception as e:
        print(f"Ошибка обработки постбэка: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
