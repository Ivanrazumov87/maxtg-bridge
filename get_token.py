"""
Получение токена MAX по номеру телефона (через SMS-код).

Запустите:  python get_token.py
Введите номер в формате +7XXXXXXXXXX, затем код из приложения/SMS MAX.
Скрипт выведет токен — вставьте его в .env как MAX_TOKEN.
"""
from max import MaxClient as Client


def main():
    phone = input("Введите номер телефона MAX (например +79991234567): ").strip()
    client = Client()
    client.auth(phone)  # запросит код и проверит его интерактивно
    print("\n=== ВАШ ТОКЕН MAX (вставьте в .env как MAX_TOKEN) ===")
    print(client.auth_token)
    print("====================================================")


if __name__ == "__main__":
    main()
