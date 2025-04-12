from faker import Faker
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import random
from datetime import datetime, timedelta
import uuid
import time

# Initialize Faker
fake = Faker()

# Cassandra connection setup
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['localhost'], port=9044, auth_provider=auth_provider)
session = cluster.connect()

# Create keyspace
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS hotel_management_3 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace('hotel_management_3')

# Create tables
print("Creating tables...")
session.execute("""
    CREATE TABLE IF NOT EXISTS hotels (
        hotel_id UUID PRIMARY KEY,
        name TEXT,
        city TEXT,
        country TEXT,
        stars INT,
        address TEXT
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS room_details (
        room_detail_id UUID PRIMARY KEY,
        name TEXT,
        description TEXT,
        room_type TEXT,
        price DECIMAL,
        capacity INT
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS rooms (
        room_id UUID PRIMARY KEY,
        hotel_id UUID,
        room_number TEXT,
        status TEXT,
        room_detail_id UUID
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS guests (
        guest_id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone TEXT,
        country TEXT
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS booking_portals (
        portal_id UUID PRIMARY KEY,
        name TEXT,
        website TEXT
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS reservations (
        reservation_id UUID PRIMARY KEY,
        guest_id UUID,
        room_id UUID,
        check_in DATE,
        check_out DATE,
        status TEXT,
        total_price DECIMAL,
        portal_id UUID
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        payment_id UUID PRIMARY KEY,
        reservation_id UUID,
        amount DECIMAL,
        method TEXT,
        status TEXT,
        payment_date TIMESTAMP
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS reviews (
        review_id UUID PRIMARY KEY,
        guest_id UUID,
        hotel_id UUID,
        rating INT,
        comment TEXT,
        review_date TIMESTAMP
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS departments (
        department_id UUID PRIMARY KEY,
        name TEXT,
        description TEXT
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        employee_id UUID PRIMARY KEY,
        hotel_id UUID,
        first_name TEXT,
        last_name TEXT,
        position TEXT,
        salary DECIMAL,
        department_id UUID
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS shift_schedules (
        shift_id UUID PRIMARY KEY,
        employee_id UUID,
        shift_date DATE,
        shift_start TIME,
        shift_end TIME,
        shift_type TEXT,
        status TEXT,
        department_id UUID
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS services (
        service_id UUID PRIMARY KEY,
        name TEXT,
        price DECIMAL
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS services_used (
        service_used_id UUID PRIMARY KEY,
        reservation_id UUID,
        service_id UUID,
        quantity INT
    )
""")

print("Tables created successfully.")

# Prepare statements for faster inserts
print("Preparing statements...")
hotels_insert = session.prepare("""
    INSERT INTO hotels (hotel_id, name, city, country, stars, address)
    VALUES (?, ?, ?, ?, ?, ?)
""")

room_details_insert = session.prepare("""
    INSERT INTO room_details (room_detail_id, name, description, room_type, price, capacity)
    VALUES (?, ?, ?, ?, ?, ?)
""")

rooms_insert = session.prepare("""
    INSERT INTO rooms (room_id, hotel_id, room_number, status, room_detail_id)
    VALUES (?, ?, ?, ?, ?)
""")

guests_insert = session.prepare("""
    INSERT INTO guests (guest_id, first_name, last_name, email, phone, country)
    VALUES (?, ?, ?, ?, ?, ?)
""")

booking_portals_insert = session.prepare("""
    INSERT INTO booking_portals (portal_id, name, website)
    VALUES (?, ?, ?)
""")

reservations_insert = session.prepare("""
    INSERT INTO reservations (reservation_id, guest_id, room_id, check_in, check_out, status, total_price, portal_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

payments_insert = session.prepare("""
    INSERT INTO payments (payment_id, reservation_id, amount, method, status, payment_date)
    VALUES (?, ?, ?, ?, ?, ?)
""")

reviews_insert = session.prepare("""
    INSERT INTO reviews (review_id, guest_id, hotel_id, rating, comment, review_date)
    VALUES (?, ?, ?, ?, ?, ?)
""")

departments_insert = session.prepare("""
    INSERT INTO departments (department_id, name, description)
    VALUES (?, ?, ?)
""")

employees_insert = session.prepare("""
    INSERT INTO employees (employee_id, hotel_id, first_name, last_name, position, salary, department_id)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""")

shift_schedules_insert = session.prepare("""
    INSERT INTO shift_schedules (shift_id, employee_id, shift_date, shift_start, shift_end, shift_type, status, department_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

services_insert = session.prepare("""
    INSERT INTO services (service_id, name, price)
    VALUES (?, ?, ?)
""")

services_used_insert = session.prepare("""
    INSERT INTO services_used (service_used_id, reservation_id, service_id, quantity)
    VALUES (?, ?, ?, ?)
""")

print("Statements prepared.")

# Generate and insert data
def generate_and_insert_data(num_records=1_000_000, batch_size=1000):
    print(f"Generating and inserting {num_records} records for each table...")
    
    # Generate IDs first for relationships
    hotel_ids = [uuid.uuid4() for _ in range(num_records)]
    room_detail_ids = [uuid.uuid4() for _ in range(num_records)]
    room_ids = [uuid.uuid4() for _ in range(num_records)]
    guest_ids = [uuid.uuid4() for _ in range(num_records)]
    portal_ids = [uuid.uuid4() for _ in range(num_records)]
    reservation_ids = [uuid.uuid4() for _ in range(num_records)]
    payment_ids = [uuid.uuid4() for _ in range(num_records)]
    review_ids = [uuid.uuid4() for _ in range(num_records)]
    department_ids = [uuid.uuid4() for _ in range(num_records)]
    employee_ids = [uuid.uuid4() for _ in range(num_records)]
    shift_ids = [uuid.uuid4() for _ in range(num_records)]
    service_ids = [uuid.uuid4() for _ in range(num_records)]
    service_used_ids = [uuid.uuid4() for _ in range(num_records)]
    
    # Track progress
    start_time = time.time()
    
    # 1. Insert hotels
    print("Inserting hotels...")
    for i in range(num_records):
        session.execute(hotels_insert, (
            hotel_ids[i],
            fake.company(),
            fake.city(),
            fake.country(),
            random.randint(1, 5),
            fake.address()
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} hotels...")
    
    # 2. Insert room_details
    print("Inserting room details...")
    room_types = ["single", "double", "suite"]
    for i in range(num_records):
        session.execute(room_details_insert, (
            room_detail_ids[i],
            fake.word() + " Room",
            fake.text(),
            random.choice(room_types),
            round(random.uniform(50, 500), 2),
            random.randint(1, 4)
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} room details...")
    
    # 3. Insert rooms
    print("Inserting rooms...")
    for i in range(num_records):
        session.execute(rooms_insert, (
            room_ids[i],
            hotel_ids[i % len(hotel_ids)],  # Use modulo to stay within bounds
            str(random.randint(100, 999)),
            random.choice(["available", "occupied"]),
            room_detail_ids[i % len(room_detail_ids)]
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} rooms...")
    
    # 4. Insert guests
    print("Inserting guests...")
    for i in range(num_records):
        session.execute(guests_insert, (
            guest_ids[i],
            fake.first_name(),
            fake.last_name(),
            fake.email(),
            fake.phone_number(),
            fake.country()
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} guests...")
    
    # 5. Insert booking portals
    print("Inserting booking portals...")
    for i in range(num_records):
        session.execute(booking_portals_insert, (
            portal_ids[i],
            fake.company(),
            fake.url()
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} booking portals...")
    
    # 6. Insert reservations
    print("Inserting reservations...")
    for i in range(num_records):
        check_in = fake.date_between(start_date="-1y", end_date="today")
        check_out = check_in + timedelta(days=random.randint(1, 14))
        
        session.execute(reservations_insert, (
            reservation_ids[i],
            guest_ids[i % len(guest_ids)],
            room_ids[i % len(room_ids)],
            check_in,
            check_out,
            random.choice(["confirmed", "cancelled", "completed"]),
            round(random.uniform(100, 5000), 2),
            portal_ids[i % len(portal_ids)]
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} reservations...")
    
    # 7. Insert payments
    print("Inserting payments...")
    for i in range(num_records):
        payment_date = fake.date_between(start_date="-1y", end_date="today")
        
        session.execute(payments_insert, (
            payment_ids[i],
            reservation_ids[i % len(reservation_ids)],
            round(random.uniform(50, 5000), 2),
            random.choice(["credit_card", "cash", "paypal"]),
            random.choice(["paid", "failed", "pending"]),
            datetime.combine(payment_date, datetime.min.time())
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} payments...")
    
    # 8. Insert reviews
    print("Inserting reviews...")
    for i in range(num_records):
        session.execute(reviews_insert, (
            review_ids[i],
            guest_ids[i % len(guest_ids)],
            hotel_ids[i % len(hotel_ids)],
            random.randint(1, 5),
            fake.text(),
            datetime.combine(fake.date_this_year(), datetime.min.time())
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} reviews...")
    
    # 9. Insert departments
    print("Inserting departments...")
    for i in range(num_records):
        session.execute(departments_insert, (
            department_ids[i],
            fake.job(),
            fake.text()
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} departments...")
    
    # 10. Insert employees
    print("Inserting employees...")
    for i in range(num_records):
        session.execute(employees_insert, (
            employee_ids[i],
            hotel_ids[i % len(hotel_ids)],
            fake.first_name(),
            fake.last_name(),
            fake.job(),
            round(random.uniform(3000, 10000), 2),
            department_ids[i % len(department_ids)]
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} employees...")
    
    # 11. Insert shift schedules
    print("Inserting shift schedules...")
    for i in range(num_records):
        session.execute(shift_schedules_insert, (
            shift_ids[i],
            employee_ids[i % len(employee_ids)],
            fake.date_this_month(),
            "08:00:00",
            "16:00:00",
            random.choice(["morning", "evening", "night"]),
            random.choice(["confirmed", "sick", "absent"]),
            department_ids[i % len(department_ids)]
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} shift schedules...")
    
    # 12. Insert services
    print("Inserting services...")
    for i in range(num_records):
        session.execute(services_insert, (
            service_ids[i],
            fake.word(),
            round(random.uniform(20, 500), 2)
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} services...")
    
    # 13. Insert services used
    print("Inserting services used...")
    for i in range(num_records):
        session.execute(services_used_insert, (
            service_used_ids[i],
            reservation_ids[i % len(reservation_ids)],
            service_ids[i % len(service_ids)],
            random.randint(1, 5)
        ))
        if i % batch_size == 0 and i > 0:
            print(f"Inserted {i} services used...")
    
    end_time = time.time()
    print(f"All data inserted successfully in {end_time - start_time:.2f} seconds!")

# Execute the data generation
generate_and_insert_data(num_records=500_000)

# Close the connection
cluster.shutdown()