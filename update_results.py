"""
Скрипт для обновления результатов проверки в исходном файле
Добавляет результаты в новые столбцы справа, сохраняя все исходные данные
"""
import pandas as pd
import sqlite3
from datetime import datetime
import os
import sys

DB_PATH = 'checks.db'

def update_file_with_results(file_id, filepath):
    """Обновляет файл результатами из БД"""
    print(f"\n{'='*60}")
    print(f"Обновляем файл результатами: {filepath}")
    print(f"File ID: {file_id}")
    print(f"{'='*60}\n")
    
    # Читаем исходный файл
    if filepath.endswith('.csv'):
        df = pd.read_csv(filepath)
    else:
        df = pd.read_excel(filepath)
    
    print(f"Загружено {len(df)} записей из файла")
    
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
    
    # Получаем результаты из БД
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT fc.phone, fc.employer_inn, c.check_id, c.status, c.offer_id, c.updated_at
        FROM file_checks fc
        JOIN checks c ON fc.check_id = c.check_id
        WHERE fc.file_id = ?
    ''', (file_id,))
    
    results = cursor.fetchall()
    conn.close()
    
    print(f"Получено {len(results)} результатов из БД\n")
    
    # Создаем словарь для быстрого поиска
    results_dict = {}
    for phone, inn, check_id, status, offer_id, updated_at in results:
        # Нормализуем телефон для поиска
        phone_key = ''.join(filter(str.isdigit, str(phone)))
        results_dict[phone_key] = {
            'check_id': check_id,
            'status': status,
            'offer_id': offer_id,
            'updated_at': updated_at
        }
    
    # Обновляем DataFrame
    updated = 0
    for index, row in df_normalized.iterrows():
        phone = str(row['телефон']).strip()
        phone_key = ''.join(filter(str.isdigit, phone))
        
        if phone_key in results_dict:
            result = results_dict[phone_key]
            df.at[index, 'Check ID'] = result['check_id']
            df.at[index, 'Статус проверки'] = result['status'] or 'pending'
            df.at[index, 'ID МФО'] = result['offer_id'] or ''
            df.at[index, 'Дата проверки'] = result['updated_at'] or ''
            updated += 1
            
            if updated % 50 == 0:
                print(f"Обновлено {updated} записей...")
    
    # Сохраняем результат
    output_path = filepath.replace('.xlsx', '_with_results.xlsx').replace('.csv', '_with_results.csv')
    if filepath.endswith('.csv'):
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
    else:
        df.to_excel(output_path, index=False, engine='openpyxl')
    
    print(f"\n{'='*60}")
    print(f"✅ ОБНОВЛЕНИЕ ЗАВЕРШЕНО!")
    print(f"{'='*60}")
    print(f"Обновлено записей: {updated}")
    print(f"Результат сохранен: {output_path}")
    print(f"{'='*60}\n")
    
    return output_path

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_id = int(sys.argv[1])
        
        # Получаем информацию о файле
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT filename FROM uploaded_files WHERE id = ?', (file_id,))
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            print(f"❌ Файл с ID {file_id} не найден в БД")
        else:
            filename = result[0]
            filepath = os.path.join('uploads', filename)
            
            if not os.path.exists(filepath):
                print(f"❌ Файл {filepath} не найден на диске")
            else:
                update_file_with_results(file_id, filepath)
    else:
        # Обновляем последний файл
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT id, filename FROM uploaded_files ORDER BY created_at DESC LIMIT 1')
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            print("❌ Файлы не найдены")
        else:
            file_id, filename = result
            filepath = os.path.join('uploads', filename)
            
            if not os.path.exists(filepath):
                print(f"❌ Файл {filepath} не найден на диске")
            else:
                update_file_with_results(file_id, filepath)
