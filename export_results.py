import pandas as pd
import sqlite3
from datetime import datetime

def export_results_to_excel(input_file, output_file=None, db_path='checks.db'):
    """
    Экспорт результатов проверок в Excel/CSV
    
    Args:
        input_file: исходный файл с клиентами
        output_file: файл для сохранения результатов (если None, добавит _results к имени)
        db_path: путь к базе данных
    """
    # Читаем исходный файл
    if input_file.endswith('.csv'):
        df = pd.read_csv(input_file)
        if output_file is None:
            output_file = input_file.replace('.csv', '_results.csv')
    else:
        df = pd.read_excel(input_file)
        if output_file is None:
            output_file = input_file.replace('.xlsx', '_results.xlsx').replace('.xls', '_results.xlsx')
    
    # Подключаемся к БД
    conn = sqlite3.connect(db_path)
    
    # Получаем результаты проверок
    results_df = pd.read_sql_query('''
        SELECT phone, employer_inn, status, offer_id, updated_at
        FROM checks
        ORDER BY updated_at DESC
    ''', conn)
    
    conn.close()
    
    # Объединяем с исходными данными по телефону и ИНН
    df['телефон'] = df['телефон'].astype(str)
    df['ИНН'] = df['ИНН'].astype(str)
    results_df['phone'] = results_df['phone'].astype(str)
    results_df['employer_inn'] = results_df['employer_inn'].astype(str)
    
    # Merge по телефону и ИНН
    merged = df.merge(
        results_df,
        left_on=['телефон', 'ИНН'],
        right_on=['phone', 'employer_inn'],
        how='left'
    )
    
    # Переименовываем колонки для удобства
    merged = merged.rename(columns={
        'status': 'Статус проверки',
        'offer_id': 'ID МФО',
        'updated_at': 'Дата проверки'
    })
    
    # Удаляем служебные колонки
    merged = merged.drop(columns=['phone', 'employer_inn'], errors='ignore')
    
    # Сохраняем результат
    if output_file.endswith('.csv'):
        merged.to_csv(output_file, index=False, encoding='utf-8-sig')
    else:
        merged.to_excel(output_file, index=False, engine='openpyxl')
    
    # Статистика
    total = len(merged)
    checked = merged['Статус проверки'].notna().sum()
    duplicates = (merged['Статус проверки'] == 'duplicate').sum()
    not_duplicates = (merged['Статус проверки'] == 'not_duplicate').sum()
    pending = (merged['Статус проверки'] == 'pending').sum()
    errors = (merged['Статус проверки'] == 'error').sum()
    
    print(f"{'='*50}")
    print(f"Результаты сохранены в: {output_file}")
    print(f"\nСтатистика:")
    print(f"  Всего записей: {total}")
    print(f"  Проверено: {checked}")
    print(f"  Дублей найдено: {duplicates}")
    print(f"  Новых клиентов: {not_duplicates}")
    print(f"  Ожидают проверки: {pending}")
    print(f"  Ошибок: {errors}")
    
    return output_file


# Пример использования
if __name__ == "__main__":
    INPUT_FILE = "clients.xlsx"  # Исходный файл
    OUTPUT_FILE = "clients_results.xlsx"  # Файл с результатами
    
    export_results_to_excel(INPUT_FILE, OUTPUT_FILE)
