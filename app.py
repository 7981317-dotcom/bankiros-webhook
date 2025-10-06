from flask import Flask, request, jsonify
import os

app = Flask(__name__)

# Простой тест что сервер работает
@app.route('/')
def home():
    return "Bankiros Webhook Service is running! 🚀"

# Основной endpoint для приема постбэков от Bankiros
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
        
        # Логируем результат (в Railway это попадет в логи)
        print(f"Получен постбэк: ID={check_id}, OfferID={offer_id}, Status={status}")
        
        # Обработка по статусу
        if status == "duplicate":
            print(f"⚠️ ДУБЛЬ ОБНАРУЖЕН! ID заявки: {check_id}, МФО: {offer_id}")
            # Здесь твоя логика: сохранить в БД, отправить уведомление и т.д.
            
        elif status == "not_duplicate":
            print(f"✅ НОВЫЙ КЛИЕНТ! ID заявки: {check_id}, МФО: {offer_id}")
            # Здесь твоя логика для нового клиента
            
        elif status == "error":
            print(f"❌ ОШИБКА ПРОВЕРКИ! ID заявки: {check_id}, МФО: {offer_id}")
            # Здесь твоя логика обработки ошибок
        
        return jsonify({"success": True, "received": True}), 200
        
    except Exception as e:
        print(f"Ошибка обработки постбэка: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 400

if __name__ == '__main__':
    # Railway автоматически устанавливает PORT
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
