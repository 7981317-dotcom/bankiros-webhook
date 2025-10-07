import pandas as pd
import requests
import time
import sqlite3
from datetime import datetime

class BulkBankirosChecker:
    def __init__(self, token, postback_url, is_production=False):
        self.token = token
        self.postback_url = postback_url
        self.base_url = "https://api.mainfin.ru" if is_production else "http://api.dev.mainfin.ru"
        self.db_path = 'checks.db'
        
    def init_db(self):
        """Инициализация базы данных"""
        conn = sqlite3.connect(self.db_path)
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
    
    def check_duplicate(self, phone, employer_inn, offer_ids):
        """Отправка одной проверки в Bankiros"""
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {self.token}"
        }
        
        payload = {
            "offerIds": offer_ids,
            "formData": {
                "phone": phone,
                "is_accepted": "1",
                "employer_inn": employer_inn
            },
            "postbackUrl": self.postback_url
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/offers_partners_v1/partner-check-phone/import",
                headers=headers,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                check_id = result.get('id')
                
                # Сохраняем в БД что отправили проверку
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO checks (check_id, phone, employer_inn, status, created_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (check_id, phone, employer_inn, 'pending', datetime.now()))
                conn.commit()
                conn.close()
                
                return {"success": True, "check_id": check_id}
            else:
                return {"success": False, "error": f"HTTP {response.status_code}"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def process_file(self, file_path, offer_ids, delay=1):
        """
        Обработка Excel или CSV файла
        
        Args:
            file_path: путь к файлу (Excel или CSV)
            offer_ids: список ID МФО для проверки [320, 321]
            delay: задержка между запросами в секундах
        """
        self.init_db()
        
        # Читаем файл
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        
        # Проверяем наличие нужных столбцов
        if 'телефон' not in df.columns or 'ИНН' not in df.columns:
            raise ValueError("В файле должны быть столбцы 'телефон' и 'ИНН'")
        
        print(f"Найдено {len(df)} записей для проверки")
        print("Начинаем отправку запросов...")
        
        results = []
        
        for index, row in df.iterrows():
            phone = str(row['телефон'])
            inn = str(row['ИНН'])
            
            print(f"[{index + 1}/{len(df)}] Проверяем: {phone}, ИНН: {inn}")
            
            result = self.check_duplicate(phone, inn, offer_ids)
            results.append(result)
            
            if result['success']:
                print(f"  ✓ Отправлено, ID проверки: {result['check_id']}")
            else:
                print(f"  ✗ Ошибка: {result['error']}")
            
            # Задержка между запросами
            time.sleep(delay)
        
        # Статистика
        success_count = sum(1 for r in results if r['success'])
        print(f"\n{'='*50}")
        print(f"Отправка завершена!")
        print(f"Успешно отправлено: {success_count}/{len(df)}")
        print(f"Ошибок: {len(df) - success_count}")
        print(f"\nРезультаты будут приходить на webhook асинхронно.")
        print(f"Используйте export_results.py для выгрузки результатов в Excel/CSV")
        
        return results


# Пример использования
if __name__ == "__main__":
    # НАСТРОЙКИ
    TOKEN = "твой_токен_bankiros"  # Замени на свой токен
    POSTBACK_URL = "https://bankiros-webhook.onrender.com/bankiros/postback"  # Твой webhook
    FILE_PATH = "clients.xlsx"  # Путь к файлу Excel или CSV
    OFFER_IDS = [320, 321]  # ID МФО для проверки (тестовые)
    IS_PRODUCTION = False  # True для production, False для тестирования
    
    # Создаем checker
    checker = BulkBankirosChecker(
        token=TOKEN,
        postback_url=POSTBACK_URL,
        is_production=IS_PRODUCTION
    )
    
    # Обрабатываем файл
    checker.process_file(FILE_PATH, OFFER_IDS, delay=1)
