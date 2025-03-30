import cassandra
print(cassandra.__version__)
from faker import Faker
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import random
from datetime import datetime, timedelta
import uuid
import time
from typing import List, Tuple, Any
from concurrent.futures import ThreadPoolExecutor

# Initialize Faker with performance optimizations
fake = Faker()
Faker.seed(42)  # For reproducible results
random.seed(42)

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(
    ['127.0.0.1'],
    port=9044,
    auth_provider=auth_provider,
    connect_timeout=60,
    idle_heartbeat_interval=30
)

# Connect with retry logic
def connect_with_retry(cluster, max_retries=3, delay=5):
    for attempt in range(max_retries):
        try:
            return cluster.connect()
        except Exception as e:
            print(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    raise Exception("Failed to connect to Cassandra after multiple attempts")

session = connect_with_retry(cluster)

# Create keyspace and tables with error handling
def execute_with_retry(session, query, max_retries=3):
    for attempt in range(max_retries):
        try:
            return session.execute(query)
        except Exception as e:
            print(f"Query attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
    raise Exception(f"Failed to execute query after {max_retries} attempts: {query}")

# Create keyspace
execute_with_retry(session, """
CREATE KEYSPACE IF NOT EXISTS hotel_management 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace('hotel_management')

# Create tables with IF NOT EXISTS
tables = [
    """
    CREATE TABLE IF NOT EXISTS hotels (
        hotel_id UUID PRIMARY KEY,
        name TEXT,
        city TEXT,
        country TEXT,
        stars INT,
        address TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS room_details (
        detail_id UUID PRIMARY KEY,
        name TEXT,
        description TEXT,
        room_type TEXT,
        price DECIMAL,
        capacity INT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rooms (
        room_id UUID PRIMARY KEY,
        hotel_id UUID,
        room_number TEXT,
        status TEXT,
        room_details UUID
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS guests (
        guest_id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone TEXT,
        country TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reservations (
        reservation_id UUID PRIMARY KEY,
        guest_id UUID,
        room_id UUID,
        check_in DATE,
        check_out DATE,
        status TEXT,
        total_price DECIMAL,
        portal_id UUID,
        portal_name TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS booking_portals (
        portal_id UUID PRIMARY KEY,
        name TEXT,
        website TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS payments (
        payment_id UUID PRIMARY KEY,
        reservation_id UUID,
        amount DECIMAL,
        method TEXT,
        status TEXT,
        payment_date DATE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reviews (
        review_id UUID PRIMARY KEY,
        guest_id UUID,
        hotel_id UUID,
        rating INT,
        comment TEXT,
        review_date TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS departments (
        department_id UUID PRIMARY KEY,
        name TEXT,
        description TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS employees (
        employee_id UUID PRIMARY KEY,
        hotel_id UUID,
        first_name TEXT,
        last_name TEXT,
        position TEXT,
        salary DECIMAL,
        department_id UUID
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shift_schedules (
        shift_id UUID PRIMARY KEY,
        employee_id UUID,
        shift_date DATE,
        shift_start TEXT,
        shift_end TEXT,
        shift_type TEXT,
        status TEXT,
        department_id UUID
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS services (
        service_id UUID PRIMARY KEY,
        name TEXT,
        price DECIMAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS services_used (
        service_used_id UUID PRIMARY KEY,
        reservation_id UUID,
        service_id UUID,
        quantity INT
    )
    """
]

for table_query in tables:
    execute_with_retry(session, table_query)

# Batch insert with improved performance
def insert_batch(session, query: str, data: List[Tuple[Any, ...]], batch_size: int = 1000) -> None:
    if len(data) >= batch_size:
        try:
            prepared = session.prepare(query)
            batch = cassandra.query.BatchStatement()
            
            for item in data[:batch_size]:
                batch.add(prepared, item)
            
            session.execute(batch)
            print(f"Inserted {batch_size} records into {query.split()[2]}")
            del data[:batch_size]
        except Exception as e:
            print(f"Error: {str(e)}")

# Data generation functions with type hints
def generate_hotels(count: int) -> List[Tuple]:
    return [
        (
            uuid.uuid4(),
            fake.unique.company(),
            fake.city(),
            fake.country(),
            random.randint(1, 5),
            fake.address()
        ) for _ in range(count)
    ]

def generate_room_details(count: int) -> List[Tuple]:
    room_types = ["single", "double", "suite", "deluxe", "family"]
    return [
        (
            uuid.uuid4(),
            f"{fake.word().capitalize()} Room",
            fake.text(max_nb_chars=200),
            random.choice(room_types),
            round(random.uniform(50, 500), 2),
            random.randint(1, 6)
        ) for _ in range(count)
    ]

def generate_rooms(count: int, hotel_ids: List[uuid.UUID], detail_ids: List[uuid.UUID]) -> List[Tuple]:
    return [
        (
            uuid.uuid4(),
            random.choice(hotel_ids),
            str(random.randint(100, 999)),
            random.choice(["available", "occupied", "maintenance"]),
            random.choice(detail_ids)
        ) for _ in range(count)
    ]

def generate_guests(count: int) -> List[Tuple]:
    return [
        (
            uuid.uuid4(),
            fake.first_name(),
            fake.last_name(),
            fake.unique.email(),
            fake.phone_number(),
            fake.country()
        ) for _ in range(count)
    ]

def generate_reservations(count: int, guest_ids: List[uuid.UUID], room_ids: List[uuid.UUID], portal_ids: List[uuid.UUID]) -> List[Tuple]:
    return [
        (
            uuid.uuid4(),
            random.choice(guest_ids),
            random.choice(room_ids),
            fake.date_between(start_date="-1y", end_date="today"),
            fake.date_between(start_date="today", end_date="+30d"),
            random.choice(["confirmed", "cancelled", "completed", "no-show"]),
            round(random.uniform(100, 5000), 2),
            random.choice(portal_ids),
            fake.company()
        ) for _ in range(count)
    ]

def generate_booking_portals(count: int) -> List[Tuple]:
    return [
        (
            uuid.uuid4(),
            fake.unique.company(),
            fake.url()
        ) for _ in range(count)
    ]

def generate_payments(count: int, reservation_ids: List[uuid.UUID]) -> List[Tuple]:
    return [
        (
            uuid.uuid4(),
            random.choice(reservation_ids),
            round(random.uniform(50, 5000), 2),
            random.choice(["credit_card", "cash", "paypal", "bank_transfer"]),
            random.choice(["paid", "failed", "pending", "refunded"]),
            fake.date_between(start_date="-1y", end_date="today")
        ) for _ in range(count)
    ]

def generate_reviews(count: int, guest_ids: List[uuid.UUID], hotel_ids: List[uuid.UUID]) -> List[Tuple]:
    return [
        (
            uuid.uuid4(),
            random.choice(guest_ids),
            random.choice(hotel_ids),
            random.randint(1, 5),
            fake.text(max_nb_chars=300),
            datetime.combine(fake.date_this_year(), datetime.min.time())
        ) for _ in range(count)
    ]

def generate_departments(count: int) -> List[Tuple]:
    dept_names = ["Front Desk", "Housekeeping", "Maintenance", "Management", "Food & Beverage", "Security"]
    return [
        (
            uuid.uuid4(),
            f"{random.choice(dept_names)} {fake.word().capitalize()}",
            fake.text(max_nb_chars=150)
        ) for _ in range(count)
    ]

def generate_employees(count: int, hotel_ids: List[uuid.UUID], dept_ids: List[uuid.UUID]) -> List[Tuple]:
    positions = ["Manager", "Supervisor", "Staff", "Assistant", "Director"]
    return [
        (
            uuid.uuid4(),
            random.choice(hotel_ids),
            fake.first_name(),
            fake.last_name(),
            f"{fake.word().capitalize()} {random.choice(positions)}",
            round(random.uniform(3000, 10000), 2),
            random.choice(dept_ids)
        ) for _ in range(count)
    ]

def generate_shift_schedules(count: int, emp_ids: List[uuid.UUID], dept_ids: List[uuid.UUID]) -> List[Tuple]:
    shift_times = [("08:00", "16:00"), ("16:00", "00:00"), ("00:00", "08:00")]
    return [
        (
            uuid.uuid4(),
            random.choice(emp_ids),
            fake.date_this_month(),
            *random.choice(shift_times),
            random.choice(["morning", "evening", "night"]),
            random.choice(["confirmed", "sick", "absent", "vacation"]),
            random.choice(dept_ids)
        ) for _ in range(count)
    ]

def generate_services(count: int) -> List[Tuple]:
    service_names = ["Breakfast", "Laundry", "Spa", "Parking", "Room Service", "Mini Bar"]
    return [
        (
            uuid.uuid4(),
            f"{random.choice(service_names)} {fake.word().capitalize()}",
            round(random.uniform(20, 500), 2)
        ) for _ in range(count)
    ]

def generate_services_used(count: int, reservation_ids: List[uuid.UUID], service_ids: List[uuid.UUID]) -> List[Tuple]:
    return [
        (
            uuid.uuid4(),
            random.choice(reservation_ids),
            random.choice(service_ids),
            random.randint(1, 5)
        ) for _ in range(count)
    ]

# Main data generation and insertion
def main():
    record_count = 500_000
    batch_size = 10
    
    # Generate all IDs first for relationships
    hotel_ids = [uuid.uuid4() for _ in range(record_count)]
    detail_ids = [uuid.uuid4() for _ in range(record_count)]
    guest_ids = [uuid.uuid4() for _ in range(record_count)]
    portal_ids = [uuid.uuid4() for _ in range(record_count)]
    dept_ids = [uuid.uuid4() for _ in range(record_count)]
    service_ids = [uuid.uuid4() for _ in range(record_count)]
    
    # Generate data in parallel
    with ThreadPoolExecutor() as executor:
        # Independent tables
        hotels_future = executor.submit(generate_hotels, record_count)
        room_details_future = executor.submit(generate_room_details, record_count)
        guests_future = executor.submit(generate_guests, record_count)
        booking_portals_future = executor.submit(generate_booking_portals, record_count)
        departments_future = executor.submit(generate_departments, record_count)
        services_future = executor.submit(generate_services, record_count)
        
        # Wait for required futures
        hotels = hotels_future.result()
        room_details = room_details_future.result()
        guests = guests_future.result()
        booking_portals = booking_portals_future.result()
        departments = departments_future.result()
        services = services_future.result()
        
        # Dependent tables
        rooms_future = executor.submit(generate_rooms, record_count, hotel_ids, detail_ids)
        reservations_future = executor.submit(generate_reservations, record_count, guest_ids, 
                                           [r[0] for r in rooms_future.result()], portal_ids)
        payments_future = executor.submit(generate_payments, record_count, 
                                        [r[0] for r in reservations_future.result()])
        reviews_future = executor.submit(generate_reviews, record_count, guest_ids, hotel_ids)
        employees_future = executor.submit(generate_employees, record_count, hotel_ids, dept_ids)
        shift_schedules_future = executor.submit(generate_shift_schedules, record_count, 
                                              [e[0] for e in employees_future.result()], dept_ids)
        services_used_future = executor.submit(generate_services_used, record_count, 
                                            [r[0] for r in reservations_future.result()], service_ids)
        
        # Get all results
        rooms = rooms_future.result()
        reservations = reservations_future.result()
        payments = payments_future.result()
        reviews = reviews_future.result()
        employees = employees_future.result()
        shift_schedules = shift_schedules_future.result()
        services_used = services_used_future.result()
    
    # Prepare queries
    queries = {
        "hotels": "INSERT INTO hotels (hotel_id, name, city, country, stars, address) VALUES (?, ?, ?, ?, ?, ?)",
        "room_details": "INSERT INTO room_details (detail_id, name, description, room_type, price, capacity) VALUES (?, ?, ?, ?, ?, ?)",
        "rooms": "INSERT INTO rooms (room_id, hotel_id, room_number, status, room_details) VALUES (?, ?, ?, ?, ?)",
        "guests": "INSERT INTO guests (guest_id, first_name, last_name, email, phone, country) VALUES (?, ?, ?, ?, ?, ?)",
        "reservations": "INSERT INTO reservations (reservation_id, guest_id, room_id, check_in, check_out, status, total_price, portal_id, portal_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        "booking_portals": "INSERT INTO booking_portals (portal_id, name, website) VALUES (?, ?, ?)",
        "payments": "INSERT INTO payments (payment_id, reservation_id, amount, method, status, payment_date) VALUES (?, ?, ?, ?, ?, ?)",
        "reviews": "INSERT INTO reviews (review_id, guest_id, hotel_id, rating, comment, review_date) VALUES (?, ?, ?, ?, ?, ?)",
        "departments": "INSERT INTO departments (department_id, name, description) VALUES (?, ?, ?)",
        "employees": "INSERT INTO employees (employee_id, hotel_id, first_name, last_name, position, salary, department_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        "shift_schedules": "INSERT INTO shift_schedules (shift_id, employee_id, shift_date, shift_start, shift_end, shift_type, status, department_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        "services": "INSERT INTO services (service_id, name, price) VALUES (?, ?, ?)",
        "services_used": "INSERT INTO services_used (service_used_id, reservation_id, service_id, quantity) VALUES (?, ?, ?, ?)"
    }
    
    # Insert data in batches
    data_sets = [
        ("hotels", hotels),
        ("room_details", room_details),
        ("rooms", rooms),
        ("guests", guests),
        ("reservations", reservations),
        ("booking_portals", booking_portals),
        ("payments", payments),
        ("reviews", reviews),
        ("departments", departments),
        ("employees", employees),
        ("shift_schedules", shift_schedules),
        ("services", services),
        ("services_used", services_used)
    ]
    
    for table_name, data in data_sets:
        query = queries[table_name]
        print(f"Inserting {len(data)} records into {table_name}...")
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            insert_batch(session, query, batch)
    
    print("All data has been successfully inserted into Cassandra!")
    cluster.shutdown()

if __name__ == "__main__":
    main()