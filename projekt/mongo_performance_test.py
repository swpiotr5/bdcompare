from pymongo import MongoClient
import time
from bson.objectid import ObjectId
from datetime import datetime

# Połączenie z MongoDB
client = MongoClient("mongodb://localhost:27018/")
db = client["hotel_management"]

def measure_time(operation_name, func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    execution_time = end_time - start_time
    log_message = f"{datetime.now()} - {operation_name} - Czas wykonania: {execution_time:.6f} sekundy"
    
    # Wyświetlanie w konsoli
    print(log_message)
    
    # Zapis do pliku
    with open("performance_results.txt", "a") as f:
        f.write(log_message + "\n")
    
    return result

# Funkcje testowe pozostają bez zmian
def count_hotels():
    return db.hotels.count_documents({})

def find_hotel():
    return db.hotels.find_one({}, {"name": 1})

def find_rooms():
    return list(db.room_details.find({"price": {"$gte": 200}}, {"name": 1, "price": 1}).limit(10))

def count_reservations():
    return db.reservations.count_documents({"check_in": {"$gte": "2023-01-01"}})

def find_services_used():
    guest_id = db.guests.find_one({}, {"_id": 1})["_id"]
    return list(db.services_used.find({"guest_id": guest_id}))

def update_reservation_status():
    guest = db.guests.find_one({}, {"_id": 1})
    if guest:
        return db.reservations.update_many({"guest_id": guest["_id"]}, {"$set": {"status": "completed"}})

def total_payments():
    result = db.payments.aggregate([
        {"$group": {"_id": None, "total_amount": {"$sum": "$amount"}}}
    ])
    return list(result)

def delete_cancelled_reservations():
    return db.reservations.delete_many({"status": "cancelled"})

# Nagłówek w pliku wynikowym
with open("performance_results.txt", "a") as f:
    f.write(f"\n\n=== Nowa sesja testowa - {datetime.now()} ===\n")

# Wykonanie testów
measure_time("Liczba hoteli", count_hotels)
measure_time("Znalezienie jednego hotelu", find_hotel)
measure_time("Wyszukanie pokoi powyżej 200$", find_rooms)
measure_time("Zliczenie rezerwacji z ostatniego roku", count_reservations)
measure_time("Pobranie usług użytych przez gościa", find_services_used)
measure_time("Aktualizacja statusu rezerwacji", update_reservation_status)
measure_time("Sumaryczna wartość płatności", total_payments)
measure_time("Usunięcie anulowanych rezerwacji", delete_cancelled_reservations)

print("Testy wydajności zakończone! Wyniki zapisane w performance_results.txt")