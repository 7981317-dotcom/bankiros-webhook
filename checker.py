import requests

class BankirosChecker:
    def __init__(self, token, is_production=False):
        self.token = token
        self.base_url = "https://api.mainfin.ru" if is_production else "http://api.dev.mainfin.ru"
        
    def check_duplicate(self, phone, employer_inn, offer_ids, postback_url):
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
            "postbackUrl": postback_url
        }
        
        response = requests.post(
            f"{self.base_url}/offers_partners_v1/partner-check-phone/import",
            headers=headers,
            json=payload
        )
        
        return response.json()

# Пример использования
checker = BankirosChecker(token="твой_токен", is_production=False)

result = checker.check_duplicate(
    phone="+7(925) 122-55-55",
    employer_inn="1234567891",
    offer_ids=[320, 321],  # Тестовые МФО
    postback_url="https://твой-домен.up.railway.app/bankiros/postback"
)

print(result)
