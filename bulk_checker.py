"""
Скрипт для массовой проверки клиентов через Bankiros API
С сохранением всех исходных данных и добавлением результатов
"""
import pandas as pd
import sqlite3
import requests
import time
from datetime import datetime
import os

# Настройки
DB_PATH = 'checks.db'
BANKIROS_TOKEN = "kOjFk444n6txgf6DFps"
BANKIROS_URL = "https://api.mainfin.ru"
POSTBACK_URL = "https://bankiros-webhook.onrender.com/bankiros/postback"
OFFER_IDS = [459]

def format_phone_for_bankiros(phone):
    """Форматирует телефон для API Bankiros в формат +7XXXXXXXXXX"""
    digits = ''.join(filter(str.isdigit, str(phone)))
    
    if digits.startswith('7') and len(digits) == 11:
        return f"+{digits}"
    elif digits.startswith('8') and len(digits) == 11:
        return f"+7{digits[1:]}"
    elif len(digits) == 10 and digits.startswith('9'):
        return f"+7{digits}"
    else:
        if not digits.startswith('+'):
            return f"+{digits}"
        return digits

def send_check_to_bankiros(phone, employer_inn, file_id):
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
            
            # Сохраняем связь с файлом
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

def process_file(file_id, filepath):
    """Обработка файла с сохранением всех данных"""
    print(f"\n{'='*60}")
    print(f"Начинаем обработку файла: {filepath}")
    print(f"File ID: {file_id}")
    print(f"{'='*60}\n")
    
    # Читаем файл
    if filepath.endswith('.csv'):
        df = pd.read_csv(filepath)
    else:
        df = pd.read_excel(filepath)
    
    print(f"Загружено {len(df)} записей")
    print(f"Столбцы: {list(df.columns)}\n")
    
    # Нормализуем названия столбцов для поиска
    df_normalized = df.copy()
    df_normalized.columns = df_normalized.columns.str.strip().str.lower()
    
    if 'телефон' not in df_normalized.columns or 'инн' not in df_normalized.columns:
        print("❌ ОШИБКА: В файле должны быть столбцы 'телефон' и 'ИНН'")
        return
    
    # Добавляем новые столбцы для результатов (если их еще нет)
    if 'Check ID' not in df.columns:
        df['Check ID'] = ''
    if 'Статус проверки' not in df.columns:
        df['Статус проверки'] = ''
    if 'ID МФО' not in df.columns:
        df['ID МФО'] = ''
    if 'Дата проверки' not in df.columns:
        df['Дата проверки'] = ''
    
    # Обновляем статус файла
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('UPDATE uploaded_files SET status = ? WHERE id = ?', ('processing', file_id))
    conn.commit()
    conn.close()
    
    sent = 0
    errors = 0
    batch_size = 10  # Обрабатываем по 10 записей с паузой
    
    for index, row in df_normalized.iterrows():
        phone = str(row['телефон']).strip()
        inn = str(row['инн']).strip()
        
        # Форматируем телефон
        phone_formatted = format_phone_for_bankiros(phone)
        
        print(f"[{index + 1}/{len(df)}] Обрабатываем: {phone} → {phone_formatted}, ИНН={inn}")
        
        result = send_check_to_bankiros(phone_formatted, inn, file_id)
        
        if result['success']:
            check_id = result.get('check_id')
            df.at[index, 'Check ID'] = check_id
            df.at[index, 'Статус проверки'] = 'pending'
            sent += 1
            print(f"  ✅ Отправлено успешно, check_id={check_id}")
        else:
            df.at[index, 'Статус проверки'] = f"Error: {result.get('error')}"
            errors += 1
            print(f"  ❌ Ошибка: {result.get('error')}")
        
        # Пауза каждые 10 запросов
        if (index + 1) % batch_size == 0:
            print(f"\n⏸️  Пауза 2 секунды... (обработано {index + 1}/{len(df)})")
            time.sleep(2)
            
            # Сохраняем промежуточный результат
            output_path = filepath.replace('.xlsx', '_progress.xlsx').replace('.csv', '_progress.csv')
            if filepath.endswith('.csv'):
                df.to_csv(output_path, index=False)
            else:
                df.to_excel(output_path, index=False)
            print(f"💾 Промежуточный результат сохранен: {output_path}\n")
    
    # Финальное сохранение
    output_path = filepath.replace('.xlsx', '_with_results.xlsx').replace('.csv', '_with_results.csv')
    if filepath.endswith('.csv'):
        df.to_csv(output_path, index=False)
    else:
        df.to_excel(output_path, index=False)
    
    # Обновляем статус файла
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE uploaded_files 
        SET status = ?, sent_count = ?, error_count = ?, completed_at = ?
        WHERE id = ?
    ''', ('completed', sent, errors, datetime.now(), file_id))
    conn.commit()
    conn.close()
    
    print(f"\n{'='*60}")
    print(f"✅ ОБРАБОТКА ЗАВЕРШЕНА!")
    print(f"{'='*60}")
    print(f"Отправлено: {sent}")
    print(f"Ошибок: {errors}")
    print(f"Результат сохранен: {output_path}")
    print(f"{'='*60}\n")
    
    return output_path

if __name__ == "__main__":
    # Получаем последний загруженный файл
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT id, filename FROM uploaded_files ORDER BY created_at DESC LIMIT 1')
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        print("❌ Файлы не найдены. Сначала загрузите файл через веб-интерфейс.")
    else:
        file_id, filename = result
        filepath = os.path.join('uploads', filename)
        
        if not os.path.exists(filepath):
            print(f"❌ Файл {filepath} не найден на диске")
        else:
            process_file(file_id, filepath)
