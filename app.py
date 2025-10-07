from flask import Flask, request, jsonify
import os
import sqlite3
from datetime import datetime

app = Flask(__name__)

# Путь к базе данных
DB_PATH = 'checks.db'

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

# Инициализируем БД при запуске
init_db()

@app.route('/')
def home():
    return "Bankiros Webhook Service is running! 🚀"

@app.route('/bankiros/postback', methods=['POST'])
def receive_postback():
    """
    Endpoint для приема постбэков от Bankiros
    """
    try:
        data = request.get_json()
        
        offer_id = data.get('offerId')
        status = data.get('status')
        check_id = data.get('id')
        
        # Логируем результат
        print(f"Получен постбэк: ID={check_id}, OfferID={offer_id}, Status={status}")
        
        # Сохраняем результат в БД
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO checks (check_id, offer_id, status, updated_at)
            VALUES (?, ?, ?, ?)
        ''', (check_id, offer_id, status, datetime.now()))
        
        conn.commit()
        conn.close()
        
        # Обработка по статусу
        if status == "duplicate":
            print(f"⚠️ ДУБЛЬ ОБНАРУЖЕН! ID заявки: {check_id}, МФО: {offer_id}")
        elif status == "not_duplicate":
            print(f"✅ НОВЫЙ КЛИЕНТ! ID заявки: {check_id}, МФО: {offer_id}")
        elif status == "error":
            print(f"❌ ОШИБКА ПРОВЕРКИ! ID заявки: {check_id}, МФО: {offer_id}")
        
        return jsonify({"success": True, "received": True}), 200
        
    except Exception as e:
        print(f"Ошибка обработки постбэка: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 400

@app.route('/stats')
def stats():
    """
    Показывает статистику проверок
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM checks')
    total = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM checks WHERE status = "duplicate"')
    duplicates = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM checks WHERE status = "not_duplicate"')
    not_duplicates = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM checks WHERE status = "error"')
    errors = cursor.fetchone()[0]
    
    conn.close()
    
    return jsonify({
        "total_checks": total,
        "duplicates": duplicates,
        "not_duplicates": not_duplicates,
        "errors": errors
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
