from faker import Faker
from pymongo import MongoClient
import random
from datetime import datetime, timedelta

# Inicjalizacja Faker
fake = Faker()

# Połączenie z MongoDB
client = MongoClient("mongodb://localhost:27018/")
db = client["hotel_management"]

def insert_batch(collection, data, batch_size=500000):
    if len(data) >= batch_size:
        try:
            result = collection.insert_many(data)
            print(f"Wstawiono {len(result.inserted_ids)} dokumentów do {collection.name}")
            data.clear()
        except Exception as e:
            print(f"Błąd w kolekcji {collection.name}: {str(e)}")

# 1. Kolekcja HOTELS
hotels_collection = db["hotels"]
hotels = []
for _ in range(500_000):
    hotel = {
        "name": fake.company(),
        "city": fake.city(),
        "country": fake.country(),
        "stars": random.randint(1, 5),
        "address": fake.address()
    }
    hotels.append(hotel)
    insert_batch(hotels_collection, hotels)

# 2. Kolekcja ROOM_DETAILS
room_details_collection = db["room_details"]
room_types = ["single", "double", "suite"]
room_details = []
for _ in range(500_000):
    detail = {
        "name": fake.word() + " Room",
        "description": fake.text(),
        "room_type": random.choice(room_types),
        "price": round(random.uniform(50, 500), 2),
        "capacity": random.randint(1, 4)
    }
    room_details.append(detail)
    insert_batch(room_details_collection, room_details)

# 3. Kolekcja ROOMS
rooms_collection = db["rooms"]
rooms = []
for _ in range(500_000):
    room = {
        "hotel_id": fake.uuid4(),
        "room_number": str(random.randint(100, 999)),
        "status": random.choice(["available", "occupied"]),
        "room_details": fake.uuid4()
    }
    rooms.append(room)
    insert_batch(rooms_collection, rooms)

# 4. Kolekcja GUESTS
guests_collection = db["guests"]
guests = []
for _ in range(500_000):
    guest = {
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "country": fake.country()
    }
    guests.append(guest)
    insert_batch(guests_collection, guests)

# 5. Kolekcja RESERVATIONS
reservations_collection = db["reservations"]
reservations = []
for _ in range(500_000):
    check_in = fake.date_between(start_date="-1y", end_date="today")
    check_out = check_in + timedelta(days=random.randint(1, 14))
    reservation = {
        "guest_id": fake.uuid4(),
        "room_id": fake.uuid4(),
        "check_in": check_in.isoformat(),  # Konwersja na string ISO
        "check_out": check_out.isoformat(),
        "status": random.choice(["confirmed", "cancelled", "completed"]),
        "total_price": round(random.uniform(100, 5000), 2),
        "portal": {"portal_id": fake.uuid4(), "name": fake.company()}
    }
    reservations.append(reservation)
    insert_batch(reservations_collection, reservations)


# 6. Kolekcja BOOKING_PORTALS
booking_portals_collection = db["booking_portals"]
booking_portals = []
for _ in range(500_000):
    portal = {"name": fake.company(), "website": fake.url()}
    booking_portals.append(portal)
    insert_batch(booking_portals_collection, booking_portals)

# 7. Kolekcja PAYMENTS
payments_collection = db["payments"]
payments = []
for _ in range(500_000):
    payment_date = fake.date_between(start_date="-1y", end_date="today")
    payment = {
        "reservation_id": fake.uuid4(),
        "amount": round(random.uniform(50, 5000), 2),
        "method": random.choice(["credit_card", "cash", "paypal"]),
        "status": random.choice(["paid", "failed", "pending"]),
        "payment_date": payment_date.isoformat()  
    }
    payments.append(payment)

insert_batch(payments_collection, payments)

# 8. Kolekcja REVIEWS
reviews_collection = db["reviews"]
reviews = []
for _ in range(500_000):
    review = {
        "guest_id": fake.uuid4(),
        "hotel_id": fake.uuid4(),
        "rating": random.randint(1, 5),
        "comment": fake.text(),
        "review_date": datetime.combine(fake.date_this_year(), datetime.min.time())
    }
    reviews.append(review)
    insert_batch(reviews_collection, reviews)

# 9. Kolekcja DEPARTMENTS
departments_collection = db["departments"]
departments = []
for _ in range(500_000):
    department = {"name": fake.job(), "description": fake.text()}
    departments.append(department)
    insert_batch(departments_collection, departments)

# 10. Kolekcja EMPLOYEES
employees_collection = db["employees"]
employees = []
for _ in range(500_000):
    employee = {
        "hotel_id": fake.uuid4(),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "position": fake.job(),
        "salary": round(random.uniform(3000, 10000), 2),
        "department_id": fake.uuid4()
    }
    employees.append(employee)
    insert_batch(employees_collection, employees)

# 11. Kolekcja SHIFT_SCHEDULES
shift_schedules_collection = db["shift_schedules"]
shift_schedules = []
for _ in range(500_000):
    shift = {
        "employee_id": fake.uuid4(),
        "shift_date": datetime.combine(fake.date_this_month(), datetime.min.time()),
        "shift_start": "08:00",
        "shift_end": "16:00",
        "shift_type": random.choice(["morning", "evening", "night"]),
        "status": random.choice(["confirmed", "sick", "absent"]),
        "department_id": fake.uuid4()
    }
    shift_schedules.append(shift)
    insert_batch(shift_schedules_collection, shift_schedules)


# 12. Kolekcja SERVICES
services_collection = db["services"]
services = []
for _ in range(500_000):
    service = {"name": fake.word(), "price": round(random.uniform(20, 500), 2)}
    services.append(service)
    insert_batch(services_collection, services)

# 13. Kolekcja SERVICES_USED
services_used_collection = db["services_used"]
services_used = []
for _ in range(500_000):
    service_used = {
        "reservation_id": fake.uuid4(),
        "service_id": fake.uuid4(),
        "quantity": random.randint(1, 5)
    }
    services_used.append(service_used)
    insert_batch(services_used_collection, services_used)

# Wstawianie pozostałych rekordów
collections = [
    (hotels_collection, hotels),
    (room_details_collection, room_details),
    (rooms_collection, rooms),
    (guests_collection, guests),
    (reservations_collection, reservations),
    (booking_portals_collection, booking_portals),
    (payments_collection, payments),
    (reviews_collection, reviews),
    (departments_collection, departments),
    (employees_collection, employees),
    (shift_schedules_collection, shift_schedules),
    (services_collection, services),
    (services_used_collection, services_used)
]

for collection, data in collections:
    if data:
        collection.insert_many(data)

print("Wszystkie dane zostały pomyślnie wstawione do MongoDB!")
