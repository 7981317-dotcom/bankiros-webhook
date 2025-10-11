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
BANKIROS_URL = "https://api.mainfin.ru"  # Production сервер
POSTBACK_URL = "https://bankiros-webhook.onrender.com/bankiros/postback"
OFFER_IDS = [459]  # ID МФО для проверки (Альфа банк РКО)

# Инициализация базы данных
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Таблица проверок
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

    # Таблица загруженных файлов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS uploaded_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            original_filename TEXT,
            records_count INTEGER,
            sent_count INTEGER,
            error_count INTEGER,
            status TEXT,  -- 'uploaded', 'processing', 'completed', 'failed'
            created_at TIMESTAMP,
            completed_at TIMESTAMP
        )
    ''')

    # Связь проверок с файлами
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER,
            check_id INTEGER,
            phone TEXT,
            employer_inn TEXT,
            FOREIGN KEY (file_id) REFERENCES uploaded_files (id),
            FOREIGN KEY (check_id) REFERENCES checks (check_id)
        )
    ''')

    conn.commit()
    conn.close()

init_db()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def format_phone_for_bankiros(phone):
    """Форматирует телефон для API Bankiros в формат +7XXXXXXXXXX"""
    # Убираем все нецифровые символы
    digits = ''.join(filter(str.isdigit, phone))

    # Если начинается с 7 и имеет 11 цифр (79XXXXXXXXX), преобразуем в +7XXXXXXXXXX
    if digits.startswith('7') and len(digits) == 11:
        return f"+{digits}"
    # Если начинается с 8 и имеет 11 цифр (89XXXXXXXXX), заменяем 8 на +7
    elif digits.startswith('8') and len(digits) == 11:
        return f"+7{digits[1:]}"
    # Если имеет 10 цифр (9XXXXXXXXX), добавляем +7
    elif len(digits) == 10 and digits.startswith('9'):
        return f"+7{digits}"
    # В остальных случаях возвращаем как есть, добавив + если нужно
    else:
        if not digits.startswith('+'):
            return f"+{digits}"
        return digits

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
        
        <!-- Раздел 3: История файлов -->
        <div class="section">
            <h2>📋 История проверок</h2>
            <p style="margin-bottom: 20px; color: #7f8c8d;">Список загруженных файлов и их статус</p>
            <div id="filesList" style="margin-bottom: 20px;">
                <p style="color: #7f8c8d;">Загружаем список файлов...</p>
            </div>
            <button class="btn btn-success" onclick="loadFiles()">
                🔄 Обновить список
            </button>
        </div>

        <!-- Раздел 4: Скачивание результатов -->
        <div class="section">
            <h2>📥 Скачать результаты</h2>
            <p style="margin-bottom: 20px; color: #7f8c8d;">Скачайте файл с результатами всех проверок</p>
            <button class="btn btn-success" onclick="downloadResults()">
                💾 Скачать все результаты (Excel)
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
            if (!confirm('Начать массовую проверку?\\n\\nПервые 100 записей будут обработаны сразу, остальные - в фоновом режиме.')) {
                return;
            }
            
            document.getElementById('sendBtn').disabled = true;
            document.getElementById('progressBar').style.display = 'block';
            showMessage('sendMessage', 'info', '⏳ Отправляем первые 100 проверок...');
            
            fetch('/api/send-checks', {
                method: 'POST'
            })
            .then(response => response.json())
            .then(data => {
                document.getElementById('progressBar').style.display = 'none';
                
                if (data.success) {
                    let message = `✅ ${data.message}\\n\\nОтправлено: ${data.sent}\\nОшибок: ${data.errors}\\nВсего записей: ${data.total}`;
                    if (data.error_details && data.error_details.length > 0) {
                        message += `\\n\\n📋 Первые ошибки:\\n${data.error_details.join('\\n')}`;
                    }
                    if (data.total > 100) {
                        message += `\\n\\n⏳ Остальные ${data.total - 100} записей обрабатываются в фоне.\\nОбновляйте список файлов для отслеживания прогресса.`;
                        // Автообновление списка файлов каждые 5 секунд
                        startAutoRefresh();
                    }
                    showMessage('sendMessage', 'success', message);
                    loadStats();
                    loadFiles();
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
        
        let autoRefreshInterval = null;
        
        function startAutoRefresh() {
            // Останавливаем предыдущий интервал если есть
            if (autoRefreshInterval) {
                clearInterval(autoRefreshInterval);
            }
            
            // Обновляем каждые 5 секунд
            autoRefreshInterval = setInterval(() => {
                loadFiles();
                loadStats();
            }, 5000);
            
            // Останавливаем через 5 минут
            setTimeout(() => {
                if (autoRefreshInterval) {
                    clearInterval(autoRefreshInterval);
                    autoRefreshInterval = null;
                }
            }, 300000);
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
        
        function loadFiles() {
            const filesList = document.getElementById('filesList');
            filesList.innerHTML = '<p style="color: #7f8c8d;">Загружаем список файлов...</p>';

            fetch('/api/files')
                .then(response => response.json())
                .then(data => {
                    if (data.files && data.files.length > 0) {
                        let html = '<div style="display: grid; gap: 15px;">';

                        data.files.forEach(file => {
                            const statusText = getStatusText(file.status);
                            const statusColor = getStatusColor(file.status);

                            html += `
                                <div style="border: 1px solid #ddd; border-radius: 10px; padding: 15px; background: white;">
                                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                                        <h4 style="margin: 0; color: #2C3E50;">${file.filename}</h4>
                                        <span style="background: ${statusColor}; color: white; padding: 5px 10px; border-radius: 5px; font-size: 0.8em;">${statusText}</span>
                                    </div>
                                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin-bottom: 15px;">
                                        <div style="text-align: center;">
                                            <div style="font-size: 1.5em; font-weight: bold; color: #667eea;">${file.records_count}</div>
                                            <div style="font-size: 0.8em; color: #7f8c8d;">Всего записей</div>
                                        </div>
                                        <div style="text-align: center;">
                                            <div style="font-size: 1.5em; font-weight: bold; color: #28a745;">${file.not_duplicates}</div>
                                            <div style="font-size: 0.8em; color: #7f8c8d;">Новых клиентов</div>
                                        </div>
                                        <div style="text-align: center;">
                                            <div style="font-size: 1.5em; font-weight: bold; color: #dc3545;">${file.duplicates}</div>
                                            <div style="font-size: 0.8em; color: #7f8c8d;">Дублей</div>
                                        </div>
                                        <div style="text-align: center;">
                                            <div style="font-size: 1.5em; font-weight: bold; color: #ffc107;">${file.pending}</div>
                                            <div style="font-size: 0.8em; color: #7f8c8d;">В обработке</div>
                                        </div>
                                    </div>
                                    <div style="display: flex; gap: 10px;">
                                        <button class="btn btn-success" style="font-size: 0.9em; padding: 8px 15px;" onclick="downloadFileResults(${file.id}, '${file.filename}')">
                                            💾 Скачать результаты
                                        </button>
                                        <div style="font-size: 0.8em; color: #7f8c8d; align-self: center;">
                                            Загружен: ${new Date(file.created_at).toLocaleString('ru-RU')}
                                        </div>
                                    </div>
                                </div>
                            `;
                        });

                        html += '</div>';
                        filesList.innerHTML = html;
                    } else {
                        filesList.innerHTML = '<p style="color: #7f8c8d;">Файлы еще не загружались</p>';
                    }
                })
                .catch(error => {
                    filesList.innerHTML = '<p style="color: #dc3545;">Ошибка загрузки списка файлов</p>';
                });
        }

        function getStatusText(status) {
            switch(status) {
                case 'uploaded': return 'Загружен';
                case 'processing': return 'Обрабатывается';
                case 'completed': return 'Готово';
                case 'failed': return 'Ошибка';
                default: return status;
            }
        }

        function getStatusColor(status) {
            switch(status) {
                case 'uploaded': return '#17a2b8';
                case 'processing': return '#ffc107';
                case 'completed': return '#28a745';
                case 'failed': return '#dc3545';
                default: return '#6c757d';
            }
        }

        function downloadFileResults(fileId, filename) {
            showMessage('downloadMessage', 'info', `⏳ Формируем результаты для ${filename}...`);
            window.location.href = `/api/download-file/${fileId}`;

            setTimeout(() => {
                showMessage('downloadMessage', 'success', '✅ Файл скачан!');
            }, 1000);
        }

        // Загружаем список файлов при открытии страницы
        loadFiles();

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
        original_filename = file.filename
        filepath = os.path.join(UPLOAD_FOLDER, f'clients_{int(datetime.now().timestamp())}.' + filename.rsplit('.', 1)[1])
        file.save(filepath)

        # Проверяем файл
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)

        # Нормализуем названия столбцов (убираем пробелы и приводим к нижнему регистру)
        df.columns = df.columns.str.strip().str.lower()

        if 'телефон' not in df.columns or 'инн' not in df.columns:
            return jsonify({
                "success": False,
                "message": "В файле должны быть столбцы 'телефон' и 'ИНН'"
            })

        records_count = len(df)

        # Сохраняем информацию о файле в БД
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO uploaded_files (filename, original_filename, records_count, status, created_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (os.path.basename(filepath), original_filename, records_count, 'uploaded', datetime.now()))
        file_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return jsonify({
            "success": True,
            "message": f"Файл '{original_filename}' успешно загружен",
            "records": records_count,
            "file_id": file_id
        })

    except Exception as e:
        return jsonify({"success": False, "message": f"Ошибка: {str(e)}"})

@app.route('/api/send-checks', methods=['POST'])
def api_send_checks():
    """API: Отправить проверки (только первые 100 записей, остальные обрабатываются фоново)"""
    try:
        # Получаем последний загруженный файл из БД
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT id, filename FROM uploaded_files ORDER BY created_at DESC LIMIT 1')
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            return jsonify({"success": False, "message": "Файл не найден. Сначала загрузите файл."})
        
        file_id, filename = result
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        
        print(f"DEBUG: file_id = {file_id}")
        print(f"DEBUG: Обрабатываем файл: {filepath}")
        
        # Проверяем существование файла
        if not os.path.exists(filepath):
            conn.close()
            return jsonify({"success": False, "message": f"Файл {filename} не найден на диске"})

        # Обновляем статус файла на 'processing'
        cursor.execute('UPDATE uploaded_files SET status = ? WHERE id = ?', ('processing', file_id))
        conn.commit()
        conn.close()

        # Читаем файл
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)

        print(f"DEBUG: Размер файла: {len(df)} строк")

        # Нормализуем названия столбцов
        df.columns = df.columns.str.strip().str.lower()

        # Проверяем наличие обязательных столбцов
        if 'телефон' not in df.columns or 'инн' not in df.columns:
            return jsonify({"success": False, "message": "В файле должны быть столбцы 'телефон' и 'ИНН'"})

        sent = 0
        errors = 0
        error_details = []
        
        # Обрабатываем только первые 100 записей в синхронном режиме
        # Остальные будут обработаны фоново
        batch_size = 100
        total_records = len(df)
        
        for index, row in df.head(batch_size).iterrows():
            phone = str(row['телефон']).strip()
            inn = str(row['инн']).strip()
            phone_formatted = format_phone_for_bankiros(phone)

            print(f"DEBUG: [{index + 1}/{batch_size}] {phone} → {phone_formatted}, ИНН={inn}")

            result = send_check_to_bankiros(phone_formatted, inn, file_id)

            if result['success']:
                sent += 1
            else:
                errors += 1
                error_details.append(f"Строка {index + 1}: {result.get('error')}")

        # Сохраняем промежуточный результат
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE uploaded_files 
            SET sent_count = ?, error_count = ?
            WHERE id = ?
        ''', (sent, errors, file_id))
        conn.commit()
        conn.close()

        # Запускаем фоновую обработку остальных записей
        if total_records > batch_size:
            import threading
            thread = threading.Thread(
                target=process_remaining_records,
                args=(file_id, filepath, batch_size)
            )
            thread.daemon = True
            thread.start()
            
            message = f"Первые {batch_size} записей отправлены! Остальные {total_records - batch_size} обрабатываются в фоне."
        else:
            # Обновляем статус на completed
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE uploaded_files 
                SET status = ?, completed_at = ?
                WHERE id = ?
            ''', ('completed', datetime.now(), file_id))
            conn.commit()
            conn.close()
            message = f"Все {total_records} записей отправлены!"

        response_data = {
            "success": True,
            "sent": sent,
            "errors": errors,
            "total": total_records,
            "message": message
        }

        if error_details:
            response_data["error_details"] = error_details[:10]

        return jsonify(response_data)

    except Exception as e:
        print(f"DEBUG: Критическая ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": f"Ошибка: {str(e)}"})

def process_remaining_records(file_id, filepath, start_index):
    """Фоновая обработка оставшихся записей"""
    try:
        print(f"DEBUG: Начинаем фоновую обработку с индекса {start_index}")
        
        # Читаем файл
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
        
        df.columns = df.columns.str.strip().str.lower()
        
        sent = 0
        errors = 0
        
        # Обрабатываем записи начиная с start_index
        for index, row in df.iloc[start_index:].iterrows():
            phone = str(row['телефон']).strip()
            inn = str(row['инн']).strip()
            phone_formatted = format_phone_for_bankiros(phone)
            
            print(f"DEBUG: Фон [{index + 1}/{len(df)}] {phone_formatted}")
            
            result = send_check_to_bankiros(phone_formatted, inn, file_id)
            
            if result['success']:
                sent += 1
            else:
                errors += 1
            
            # Обновляем прогресс каждые 10 записей
            if (index - start_index + 1) % 10 == 0:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE uploaded_files 
                    SET sent_count = sent_count + ?, error_count = error_count + ?
                    WHERE id = ?
                ''', (sent, errors, file_id))
                conn.commit()
                conn.close()
                sent = 0
                errors = 0
                
                # Пауза чтобы не перегружать API
                import time
                time.sleep(1)
        
        # Финальное обновление
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE uploaded_files 
            SET status = ?, sent_count = sent_count + ?, error_count = error_count + ?, completed_at = ?
            WHERE id = ?
        ''', ('completed', sent, errors, datetime.now(), file_id))
        conn.commit()
        conn.close()
        
        print(f"DEBUG: Фоновая обработка завершена. Отправлено: {sent}, ошибок: {errors}")
        
    except Exception as e:
        print(f"DEBUG: Ошибка в фоновой обработке: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Помечаем файл как failed
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('UPDATE uploaded_files SET status = ? WHERE id = ?', ('failed', file_id))
        conn.commit()
        conn.close()

def send_check_to_bankiros(phone, employer_inn, file_id=None):
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

            # Сохраняем связь с файлом, если указан file_id
            if file_id:
                cursor.execute('''
                    INSERT INTO file_checks (file_id, check_id, phone, employer_inn)
                    VALUES (?, ?, ?, ?)
                ''', (file_id, check_id, phone, employer_inn))

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

@app.route('/api/files')
def api_files():
    """API: Получить список загруженных файлов"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, original_filename, records_count, sent_count, error_count,
                   status, created_at, completed_at
            FROM uploaded_files
            ORDER BY created_at DESC
            LIMIT 10
        ''')

        files = []
        for row in cursor.fetchall():
            file_id, original_filename, records_count, sent_count, error_count, status, created_at, completed_at = row

            # Получаем статистику по файлу
            cursor.execute('''
                SELECT COUNT(*) FROM file_checks fc
                JOIN checks c ON fc.check_id = c.check_id
                WHERE fc.file_id = ? AND c.status = 'duplicate'
            ''', (file_id,))
            duplicates = cursor.fetchone()[0]

            cursor.execute('''
                SELECT COUNT(*) FROM file_checks fc
                JOIN checks c ON fc.check_id = c.check_id
                WHERE fc.file_id = ? AND c.status = 'not_duplicate'
            ''', (file_id,))
            not_duplicates = cursor.fetchone()[0]

            cursor.execute('''
                SELECT COUNT(*) FROM file_checks fc
                JOIN checks c ON fc.check_id = c.check_id
                WHERE fc.file_id = ? AND (c.status = 'pending' OR c.status IS NULL)
            ''', (file_id,))
            pending = cursor.fetchone()[0]

            files.append({
                "id": file_id,
                "filename": original_filename,
                "records_count": records_count,
                "sent_count": sent_count or 0,
                "error_count": error_count or 0,
                "status": status,
                "created_at": created_at,
                "completed_at": completed_at,
                "duplicates": duplicates,
                "not_duplicates": not_duplicates,
                "pending": pending
            })

        conn.close()
        return jsonify({"files": files})

    except Exception as e:
        return jsonify({"success": False, "message": f"Ошибка: {str(e)}"})

@app.route('/api/download-file/<int:file_id>')
def api_download_file(file_id):
    """API: Скачать результаты по конкретному файлу с сохранением всех исходных данных"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Получаем информацию о файле
        cursor.execute('SELECT filename, original_filename FROM uploaded_files WHERE id = ?', (file_id,))
        result = cursor.fetchone()
        if not result:
            conn.close()
            return jsonify({"success": False, "message": "Файл не найден"})

        filename, original_filename = result
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        
        # Проверяем существование файла
        if not os.path.exists(filepath):
            conn.close()
            return jsonify({"success": False, "message": "Исходный файл не найден на диске"})
        
        # Читаем исходный файл
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
        
        # Нормализуем столбцы для поиска
        df_normalized = df.copy()
        df_normalized.columns = df_normalized.columns.str.strip().str.lower()
        
        # Добавляем столбцы для результатов (если их нет)
        if 'Check ID' not in df.columns:
            df['Check ID'] = ''
        if 'Статус проверки' not in df.columns:
            df['Статус проверки'] = ''
        if 'ID МФО' not in df.columns:
            df['ID МФО'] = ''
        if 'Дата проверки' not in df.columns:
            df['Дата проверки'] = ''
        
        # Получаем результаты из БД (включая employer_inn для точного матчинга)
        cursor.execute('''
            SELECT fc.phone, fc.employer_inn, c.check_id, c.status, c.offer_id, c.updated_at
            FROM file_checks fc
            JOIN checks c ON fc.check_id = c.check_id
            WHERE fc.file_id = ?
        ''', (file_id,))
        
        results = cursor.fetchall()
        conn.close()
        
        # Создаем словарь для быстрого поиска по (phone_key, inn)
        results_dict = {}
        for phone, inn, check_id, status, offer_id, updated_at in results:
            phone_key = ''.join(filter(str.isdigit, str(phone)))
            inn_key = str(inn).strip()
            composite_key = (phone_key, inn_key)
            results_dict[composite_key] = {
                'check_id': check_id,
                'status': status,
                'offer_id': offer_id,
                'updated_at': updated_at
            }
        
        # Обновляем DataFrame результатами
        updated_count = 0
        for index, row in df_normalized.iterrows():
            phone = str(row['телефон']).strip()
            inn = str(row['инн']).strip()
            phone_key = ''.join(filter(str.isdigit, phone))
            composite_key = (phone_key, inn)
            
            if composite_key in results_dict:
                result = results_dict[composite_key]
                df.at[index, 'Check ID'] = result['check_id']
                df.at[index, 'Статус проверки'] = result['status'] or 'pending'
                df.at[index, 'ID МФО'] = result['offer_id'] or ''
                df.at[index, 'Дата проверки'] = result['updated_at'] or ''
                updated_count += 1
        
        print(f"DEBUG: Обновлено {updated_count} записей из {len(results)} результатов в БД")

        # Создаем Excel в памяти
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Результаты')
        output.seek(0)

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'{original_filename}_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
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
        cursor.execute('DELETE FROM uploaded_files')
        cursor.execute('DELETE FROM file_checks')
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
