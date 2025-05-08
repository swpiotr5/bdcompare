import random
import time
import psycopg2
from faker import Faker
from datetime import timedelta, datetime
class PostgreSQLConnector:

    def __init__(self, dbname, user, password, host, **kwargs):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.table_queries = [
    """
    CREATE TABLE IF NOT EXISTS hotels (
        hotel_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        city VARCHAR(100) NOT NULL,
        country VARCHAR(100) NOT NULL,
        stars SMALLINT CHECK (stars BETWEEN 1 AND 5),
        address TEXT
    )
    """,
"""
    CREATE TABLE IF NOT EXISTS room_details (
        room_detail_id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        description TEXT,
        room_type VARCHAR(50) NOT NULL,
        price DECIMAL(10,2),
        capacity INT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rooms (
        room_id SERIAL PRIMARY KEY,
        hotel_id INT NOT NULL,
        room_number VARCHAR(10) NOT NULL,
        status VARCHAR(50),
        room_detail_id INT,
        FOREIGN KEY (hotel_id) REFERENCES hotels(hotel_id),
        FOREIGN KEY (room_detail_id) REFERENCES room_details(room_detail_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS guests (
        guest_id SERIAL PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        email VARCHAR(150) UNIQUE NOT NULL,
        phone VARCHAR(50),
        country VARCHAR(100)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS booking_portals (
        portal_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        website VARCHAR(255)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reservations (
        reservation_id SERIAL PRIMARY KEY,
        guest_id INT NOT NULL,
        room_id INT NOT NULL,
        check_in DATE NOT NULL,
        check_out DATE NOT NULL,
        status VARCHAR(50) DEFAULT 'confirmed',
        total_price NUMERIC(10,2) NOT NULL,
        portal_id INT,
        FOREIGN KEY (guest_id) REFERENCES guests(guest_id),
        FOREIGN KEY (room_id) REFERENCES rooms(room_id),
        FOREIGN KEY (portal_id) REFERENCES booking_portals(portal_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS payments (
        payment_id SERIAL PRIMARY KEY,
        reservation_id INT NOT NULL,
        amount NUMERIC(10,2) NOT NULL,
        method VARCHAR(50) NOT NULL,
        status VARCHAR(50) DEFAULT 'paid',
        payment_date DATE NOT NULL,
        FOREIGN KEY (reservation_id) REFERENCES reservations(reservation_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reviews (
        review_id SERIAL PRIMARY KEY,
        guest_id INT NOT NULL,
        hotel_id INT NOT NULL,
        rating SMALLINT CHECK (rating BETWEEN 1 AND 5),
        comment TEXT,
        review_date DATE,
        FOREIGN KEY (guest_id) REFERENCES guests(guest_id),
        FOREIGN KEY (hotel_id) REFERENCES hotels(hotel_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS departments (
        department_id SERIAL PRIMARY KEY,
        name VARCHAR(100) UNIQUE,
        description TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS employees (
        employee_id SERIAL PRIMARY KEY,
        hotel_id INT NOT NULL,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        position VARCHAR(100) NOT NULL,
        salary NUMERIC(10,2) NOT NULL,
        department_id INT,
        FOREIGN KEY (hotel_id) REFERENCES hotels(hotel_id),
        FOREIGN KEY (department_id) REFERENCES departments(department_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shift_schedules (
        schedule_id SERIAL PRIMARY KEY,
        employee_id INT NOT NULL,
        shift_date DATE NOT NULL,
        shift_start TIME NOT NULL,
        shift_end TIME NOT NULL,
        shift_type VARCHAR(50),
        status VARCHAR(50) DEFAULT 'confirmed',
        department_id INT,
        FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
        FOREIGN KEY (department_id) REFERENCES departments(department_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS services (
        service_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        price NUMERIC(10,2) NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS services_used (
        id SERIAL PRIMARY KEY,
        reservation_id INT NOT NULL,
        service_id INT NOT NULL,
        quantity INT DEFAULT 1,
        FOREIGN KEY (reservation_id) REFERENCES reservations(reservation_id),
        FOREIGN KEY (service_id) REFERENCES services(service_id)
    )
    """
]

    def connect(self):
        try:
            return psycopg2.connect(
                host=self.host,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
        except psycopg2.OperationalError as e:
            print("Błąd połączenia z bazą danych:", e)
            return None

    def create_tables(self):
        conn = self.connect()
        cursor = conn.cursor()

        # sprawdzamy czy tabela 'hotels' istnieje
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'hotels'
            )
        """)
        (exists,) = cursor.fetchone()

        if exists:
            print("Tabele już istnieją. Pomijam tworzenie.")
        else:
            try:
                for query in self.table_queries:
                    cursor.execute(query)
                    conn.commit()
                print("Tabele zostały utworzone w PostgreSQL.")
            except psycopg2.OperationalError as e:
                print("Error occurred while creating tables: ", e)
        cursor.close()
        conn.close()

    def drop_tables(self):
        conn = self.connect()
        cursor = conn.cursor()

        tables = [
            "services_used",
            "shift_schedules",
            "employees",
            "services",
            "reviews",
            "payments",
            "reservations",
            "booking_portals",
            "guests",
            "rooms",
            "room_details",
            "departments",
            "hotels"
        ]

        for table in tables:
            print(f"Usuwam tabelę: {table}")
            cursor.execute(f'DROP TABLE IF EXISTS {table} CASCADE')

        conn.commit()
        cursor.close()
        conn.close()
        print("Wszystkie tabele zostały usunięte.")

    def insert_fake_data(self):
        from faker import Faker
        import random
        from datetime import timedelta, datetime
        import time

        fake = Faker()
        start_time = time.time()
        conn = self.connect()
        cursor = conn.cursor()

        def batch_insert(query, data):
            cursor.executemany(query, data)
            conn.commit()

        BATCH_SIZE = 10_000
        TARGET_ROWS = 500_000

        print("🔄 Wstawianie danych...")

        # Hotels
        print("🏨 Hotels:")
        for i in range(0, TARGET_ROWS, BATCH_SIZE):
            hotels = [(fake.company(), fake.city(), fake.country(), random.randint(1, 5), fake.address()) for _ in
                      range(BATCH_SIZE)]
            batch_insert("""
                INSERT INTO hotels (name, city, country, stars, address)
                VALUES (%s, %s, %s, %s, %s)
            """, hotels)
            print(f"  ➤ {i + BATCH_SIZE} / {TARGET_ROWS}")

        # Room Details
        print("🛏️ Room Details:")
        for i in range(0, TARGET_ROWS, BATCH_SIZE):
            room_details = [(fake.word(), fake.text(), random.choice(['single', 'double', 'suite']),
                             round(random.uniform(50, 300), 2), random.randint(1, 5)) for _ in range(BATCH_SIZE)]
            batch_insert("""
                INSERT INTO room_details (name, description, room_type, price, capacity)
                VALUES (%s, %s, %s, %s, %s)
            """, room_details)
            print(f"  ➤ {i + BATCH_SIZE} / {TARGET_ROWS}")

        # Rooms
        print("🚪 Rooms:")
        for i in range(0, TARGET_ROWS, BATCH_SIZE):
            rooms = [(random.randint(1, TARGET_ROWS), str(random.randint(100, 999)),
                      random.choice(['available', 'booked']), random.randint(1, TARGET_ROWS)) for _ in
                     range(BATCH_SIZE)]
            batch_insert("""
                INSERT INTO rooms (hotel_id, room_number, status, room_detail_id)
                VALUES (%s, %s, %s, %s)
            """, rooms)
            print(f"  ➤ {i + BATCH_SIZE} / {TARGET_ROWS}")

        # Guests
        print("🧍 Guests:")
        for i in range(0, TARGET_ROWS, BATCH_SIZE):
            guests = [(fake.first_name(), fake.last_name(), fake.unique.email(), fake.phone_number(), fake.country())
                      for _ in range(BATCH_SIZE)]
            batch_insert("""
                INSERT INTO guests (first_name, last_name, email, phone, country)
                VALUES (%s, %s, %s, %s, %s)
            """, guests)
            print(f"  ➤ {i + BATCH_SIZE} / {TARGET_ROWS}")

        # Booking Portals
        print("🌐 Booking Portals:")
        for i in range(0, TARGET_ROWS, BATCH_SIZE):
            portals = [(fake.company(), fake.url()) for _ in range(BATCH_SIZE)]
            batch_insert("""
                INSERT INTO booking_portals (name, website)
                VALUES (%s, %s)
            """, portals)
            print(f"  ➤ {i + BATCH_SIZE} / {TARGET_ROWS}")

        # Reservations
        print("📅 Reservations:")
        for i in range(0, TARGET_ROWS, BATCH_SIZE):
            reservations = []
            for _ in range(BATCH_SIZE):
                check_in = fake.date_between(start_date='-2y', end_date='today')
                check_out = check_in + timedelta(days=random.randint(1, 14))
                reservations.append((
                    random.randint(1, TARGET_ROWS),
                    random.randint(1, TARGET_ROWS),
                    check_in,
                    check_out,
                    random.choice(['confirmed', 'cancelled', 'completed']),
                    round(random.uniform(100, 3000), 2),
                    random.randint(1, TARGET_ROWS)
                ))
            batch_insert("""
                INSERT INTO reservations (guest_id, room_id, check_in, check_out, status, total_price, portal_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, reservations)
            print(f"  ➤ {i + BATCH_SIZE} / {TARGET_ROWS}")

        # Payments
        print("💳 Payments:")
        for i in range(1, TARGET_ROWS + 1, BATCH_SIZE):
            payments = [(reservation_id,
                         round(random.uniform(100, 3000), 2),
                         random.choice(['card', 'cash', 'transfer']),
                         random.choice(['paid', 'pending']),
                         fake.date_between(start_date='-2y', end_date='today'))
                        for reservation_id in range(i, i + BATCH_SIZE)]
            batch_insert("""
                INSERT INTO payments (reservation_id, amount, method, status, payment_date)
                VALUES (%s, %s, %s, %s, %s)
            """, payments)
            print(f"  ➤ {min(i + BATCH_SIZE - 1, TARGET_ROWS)} / {TARGET_ROWS}")

        # Reviews
        print("⭐ Reviews:")
        for i in range(0, TARGET_ROWS, BATCH_SIZE):
            reviews = [(random.randint(1, TARGET_ROWS),
                        random.randint(1, TARGET_ROWS),
                        random.randint(1, 5),
                        fake.text(),
                        fake.date_between(start_date='-2y', end_date='today')) for _ in range(BATCH_SIZE)]
            batch_insert("""
                INSERT INTO reviews (guest_id, hotel_id, rating, comment, review_date)
                VALUES (%s, %s, %s, %s, %s)
            """, reviews)
            print(f"  ➤ {i + BATCH_SIZE} / {TARGET_ROWS}")

        # Departments (stałe dane)
        departments = ["Recepcja", "Sprzątanie", "Kuchnia", "Obsługa klienta", "Parking", "Bar"]
        print("🏢 Departments:")
        dept_data = [(dept, fake.text()) for dept in departments]
        batch_insert("""
            INSERT INTO departments (name, description)
            VALUES (%s, %s)
        """, dept_data)

        # Employees
        print("👨‍💼 Employees:")
        for i in range(0, TARGET_ROWS, BATCH_SIZE):
            employees = [(random.randint(1, TARGET_ROWS),
                          fake.first_name(),
                          fake.last_name(),
                          fake.job(),
                          round(random.uniform(2500, 8000), 2),
                          random.randint(1, len(departments))) for _ in range(BATCH_SIZE)]
            batch_insert("""
                INSERT INTO employees (hotel_id, first_name, last_name, position, salary, department_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, employees)
            print(f"  ➤ {i + BATCH_SIZE} / {TARGET_ROWS}")

        # Shift Schedules
        print("📆 Shift Schedules:")
        for i in range(0, TARGET_ROWS, BATCH_SIZE):
            schedules = []
            for _ in range(BATCH_SIZE):
                shift_start = datetime.strptime(f"{random.randint(6, 12)}:00", "%H:%M").time()
                shift_end = datetime.strptime(f"{random.randint(13, 23)}:00", "%H:%M").time()
                schedules.append((
                    random.randint(1, TARGET_ROWS),
                    fake.date_between(start_date='-1y', end_date='+1y'),
                    shift_start,
                    shift_end,
                    random.choice(['morning', 'evening', 'night']),
                    random.choice(['confirmed', 'pending']),
                    random.randint(1, len(departments))
                ))
            batch_insert("""
                INSERT INTO shift_schedules (employee_id, shift_date, shift_start, shift_end, shift_type, status, department_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, schedules)
            print(f"  ➤ {i + BATCH_SIZE} / {TARGET_ROWS}")

        # Services (stałe dane)
        services = ["Wi-Fi", "Parking", "Śniadanie", "Obiady", "Siłownia", "SPA", "Bilard", "Basen", "Rowery"]
        print("🛎️ Services:")
        services_data = [(s, round(random.uniform(10, 100), 2)) for s in services]
        batch_insert("""
            INSERT INTO services (name, price)
            VALUES (%s, %s)
        """, services_data)

        # Services Used
        print("🧾 Services Used:")
        for i in range(0, TARGET_ROWS, BATCH_SIZE):
            services_used = [(random.randint(1, TARGET_ROWS), random.randint(1, len(services)), random.randint(1, 3))
                             for _ in range(BATCH_SIZE)]
            batch_insert("""
                INSERT INTO services_used (reservation_id, service_id, quantity)
                VALUES (%s, %s, %s)
            """, services_used)
            print(f"  ➤ {i + BATCH_SIZE} / {TARGET_ROWS}")

        conn.close()
        end_time = time.time()
        print(
            f"✅ Wszystkie dane (500k x tabela) zostały wstawione. Czas wykonania: {end_time - start_time:.2f} sekund.")

    # def insert_fake_data(self):
    #     start_time = time.time()
    #     fake = Faker()
    #     conn = self.connect()
    #     cursor = conn.cursor()
    #
    #     # Hotels
    #     for _ in range(1000):
    #         cursor.execute("""
    #                INSERT INTO hotels (name, city, country, stars, address)
    #                VALUES (%s, %s, %s, %s, %s)
    #            """, (fake.company(), fake.city(), fake.country(), random.randint(1, 5), fake.address()))
    #
    #     # Room Details
    #     for _ in range(1000):
    #         cursor.execute("""
    #                INSERT INTO room_details (name, description, room_type, price, capacity)
    #                VALUES (%s, %s, %s, %s, %s)
    #            """, (
    #         fake.word(), fake.text(), random.choice(['single', 'double', 'suite']), round(random.uniform(50, 300), 2),
    #         random.randint(1, 5)))
    #
    #     # Rooms
    #     for _ in range(5000):
    #         cursor.execute("""
    #                INSERT INTO rooms (hotel_id, room_number, status, room_detail_id)
    #                VALUES (%s, %s, %s, %s)
    #            """, (random.randint(1, 1000), str(random.randint(100, 999)), random.choice(['available', 'booked']),
    #                  random.randint(1, 1000)))
    #
    #     # Guests
    #     for _ in range(100000):
    #         cursor.execute("""
    #                INSERT INTO guests (first_name, last_name, email, phone, country)
    #                VALUES (%s, %s, %s, %s, %s)
    #            """, (fake.first_name(), fake.last_name(), fake.unique.email(), fake.phone_number(), fake.country()))
    #
    #     # Booking Portals
    #     for _ in range(50):
    #         cursor.execute("""
    #                INSERT INTO booking_portals (name, website)
    #                VALUES (%s, %s)
    #            """, (fake.company(), fake.url()))
    #
    #     # Reservations
    #     for _ in range(250000):
    #         check_in = fake.date_between(start_date='-2y', end_date='today')
    #         check_out = check_in + timedelta(days=random.randint(1, 14))
    #         cursor.execute("""
    #                INSERT INTO reservations (guest_id, room_id, check_in, check_out, status, total_price, portal_id)
    #                VALUES (%s, %s, %s, %s, %s, %s, %s)
    #            """, (
    #             random.randint(1, 100000),
    #             random.randint(1, 5000),
    #             check_in,
    #             check_out,
    #             random.choice(['confirmed', 'cancelled', 'completed']),
    #             round(random.uniform(100, 3000), 2),
    #             random.randint(1, 50)
    #         ))
    #
    #     # Payments
    #     for reservation_id in range(1, 250001):
    #         cursor.execute("""
    #                INSERT INTO payments (reservation_id, amount, method, status, payment_date)
    #                VALUES (%s, %s, %s, %s, %s)
    #            """, (reservation_id, round(random.uniform(100, 3000), 2), random.choice(['card', 'cash', 'transfer']),
    #                  random.choice(['paid', 'pending']), fake.date_between(start_date='-2y', end_date='today')))
    #
    #     # Reviews
    #     for _ in range(50000):
    #         cursor.execute("""
    #                INSERT INTO reviews (guest_id, hotel_id, rating, comment, review_date)
    #                VALUES (%s, %s, %s, %s, %s)
    #            """, (random.randint(1, 100000), random.randint(1, 1000), random.randint(1, 5), fake.text(),
    #                  fake.date_between(start_date='-2y', end_date='today')))
    #
    #     # Departments
    #     departments = ["Recepcja", "Sprzątanie", "Kuchnia", "Obsługa klienta", "Parking", "Bar"]
    #     for dept in departments:
    #         cursor.execute("""
    #                INSERT INTO departments (name, description)
    #                VALUES (%s, %s)
    #            """, (dept, fake.text()))
    #
    #     # Employees
    #     for _ in range(5000):
    #         cursor.execute("""
    #                INSERT INTO employees (hotel_id, first_name, last_name, position, salary, department_id)
    #                VALUES (%s, %s, %s, %s, %s, %s)
    #            """, (
    #             random.randint(1, 1000),
    #             fake.first_name(),
    #             fake.last_name(),
    #             fake.job(),
    #             round(random.uniform(2500, 8000), 2),
    #             random.randint(1, len(departments))
    #         ))
    #
    #     # Shift Schedules
    #     for _ in range(100000):
    #         shift_start = datetime.strptime(f"{random.randint(6, 12)}:00", "%H:%M").time()
    #         shift_end = datetime.strptime(f"{random.randint(13, 23)}:00", "%H:%M").time()
    #         cursor.execute("""
    #                INSERT INTO shift_schedules (employee_id, shift_date, shift_start, shift_end, shift_type, status, department_id)
    #                VALUES (%s, %s, %s, %s, %s, %s, %s)
    #            """, (
    #             random.randint(1, 5000),
    #             fake.date_between(start_date='-1y', end_date='+1y'),
    #             shift_start,
    #             shift_end,
    #             random.choice(['morning', 'evening', 'night']),
    #             random.choice(['confirmed', 'pending']),
    #             random.randint(1, len(departments))
    #         ))
    #
    #     # Services
    #     services = ["Wi-Fi", "Parking", "Śniadanie", "Obiady", "Siłownia", "SPA", "Bilard", "Basen", "Rowery"]
    #     for service in services:
    #         cursor.execute("""
    #                INSERT INTO services (name, price)
    #                VALUES (%s, %s)
    #            """, (service, round(random.uniform(10, 100), 2)))
    #
    #     # Services Used
    #     for _ in range(50000):
    #         cursor.execute("""
    #                INSERT INTO services_used (reservation_id, service_id, quantity)
    #                VALUES (%s, %s, %s)
    #            """, (
    #             random.randint(1, 250000),
    #             random.randint(1, len(services)),
    #             random.randint(1, 3)
    #         ))
    #
    #     conn.commit()
    #     conn.close()
    #
    #     end_time = time.time()
    #     print(f"✅ Wszystkie dane zostały wstawione pomyślnie. Czas wykonania: {end_time - start_time:.2f} sekund.")

# def insert_data():
#     try:
#         conn = psycopg2.connect("dbname=mydb user=user password=password host=localhost")
#         cursor = conn.cursor()
#
#         for query in create_table_queries_postgres:
#             cursor.execute(query)
#             conn.commit()
#
#         def timed_block(label, fn):
#             start = time.time()
#             fn()
#             end = time.time()
#             print(f"{label} - czas wykonania: {end - start:.3f} sekundy")
#
#         # --- Wstawianie danych z pomiarem czasu ---
#
#         hotel_ids = []
#         def insert_hotels():
#             for _ in range(10):
#                 name = fake.company()
#                 city = fake.city()
#                 country = fake.country()
#                 stars = random.randint(2, 5)
#                 address = fake.address().replace("\n", ", ")
#                 cursor.execute("""
#                     INSERT INTO hotels (name, city, country, stars, address)
#                     VALUES (%s, %s, %s, %s, %s) RETURNING hotel_id
#                 """, (name, city, country, stars, address))
#                 hotel_ids.append(cursor.fetchone()[0])
#         timed_block("Wstawianie hoteli", insert_hotels)
#
#         room_detail_ids = []
#         def insert_room_details():
#             room_types = ['single', 'double', 'suite']
#             for _ in range(10):
#                 name = f"{random.choice(room_types).capitalize()} Room"
#                 description = fake.text(max_nb_chars=100)
#                 room_type = random.choice(room_types)
#                 price = round(random.uniform(50, 300), 2)
#                 capacity = random.randint(1, 4)
#                 cursor.execute("""
#                     INSERT INTO room_details (name, description, room_type, price, capacity)
#                     VALUES (%s, %s, %s, %s, %s) RETURNING room_detail_id
#                 """, (name, description, room_type, price, capacity))
#                 room_detail_ids.append(cursor.fetchone()[0])
#         timed_block("Wstawianie szczegółów pokoi", insert_room_details)
#
#         room_ids = []
#         def insert_rooms():
#             for hotel_id in hotel_ids:
#                 for _ in range(3):
#                     room_number = str(fake.random_int(min=100, max=999))
#                     status = random.choice(['available', 'occupied', 'maintenance'])
#                     detail_id = random.choice(room_detail_ids)
#                     cursor.execute("""
#                         INSERT INTO rooms (hotel_id, room_number, status, room_detail_id)
#                         VALUES (%s, %s, %s, %s) RETURNING room_id
#                     """, (hotel_id, room_number, status, detail_id))
#                     room_ids.append(cursor.fetchone()[0])
#         timed_block("Wstawianie pokoi", insert_rooms)
#
#         guest_ids = []
#         def insert_guests():
#             for _ in range(10):
#                 first = fake.first_name()
#                 last = fake.last_name()
#                 email = fake.unique.email()
#                 phone = fake.phone_number()
#                 country = fake.country()
#                 cursor.execute("""
#                     INSERT INTO guests (first_name, last_name, email, phone, country)
#                     VALUES (%s, %s, %s, %s, %s) RETURNING guest_id
#                 """, (first, last, email, phone, country))
#                 guest_ids.append(cursor.fetchone()[0])
#         timed_block("Wstawianie gości", insert_guests)
#
#         portal_ids = []
#         def insert_booking_portals():
#             for name in ['Booking.com', 'Expedia', 'Airbnb']:
#                 website = f"https://www.{name.lower().replace('.', '')}.com"
#                 cursor.execute("""
#                     INSERT INTO booking_portals (name, website)
#                     VALUES (%s, %s) RETURNING portal_id
#                 """, (name, website))
#                 portal_ids.append(cursor.fetchone()[0])
#         timed_block("Wstawianie portali rezerwacyjnych", insert_booking_portals)
#
#         reservation_ids = []
#         def insert_reservations():
#             for _ in range(15):
#                 guest_id = random.choice(guest_ids)
#                 room_id = random.choice(room_ids)
#                 check_in = fake.date_between(start_date='-30d', end_date='today')
#                 check_out = check_in + timedelta(days=random.randint(1, 7))
#                 status = random.choice(['confirmed', 'cancelled', 'completed'])
#                 total_price = round(random.uniform(100, 1500), 2)
#                 portal_id = random.choice(portal_ids)
#                 cursor.execute("""
#                     INSERT INTO reservations (guest_id, room_id, check_in, check_out, status, total_price, portal_id)
#                     VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING reservation_id
#                 """, (guest_id, room_id, check_in, check_out, status, total_price, portal_id))
#                 reservation_ids.append(cursor.fetchone()[0])
#         timed_block("Wstawianie rezerwacji", insert_reservations)
#
#         def insert_payments():
#             for reservation_id in reservation_ids:
#                 amount = round(random.uniform(100, 1500), 2)
#                 method = random.choice(['credit card', 'paypal', 'cash'])
#                 status = random.choice(['paid', 'pending'])
#                 payment_date = fake.date_between(start_date='-30d', end_date='today')
#                 cursor.execute("""
#                     INSERT INTO payments (reservation_id, amount, method, status, payment_date)
#                     VALUES (%s, %s, %s, %s, %s)
#                 """, (reservation_id, amount, method, status, payment_date))
#         timed_block("Wstawianie płatności", insert_payments)
#
#         def insert_reviews():
#             for _ in range(10):
#                 guest_id = random.choice(guest_ids)
#                 hotel_id = random.choice(hotel_ids)
#                 rating = random.randint(1, 5)
#                 comment = fake.sentence()
#                 review_date = fake.date_between(start_date='-60d', end_date='today')
#                 cursor.execute("""
#                     INSERT INTO reviews (guest_id, hotel_id, rating, comment, review_date)
#                     VALUES (%s, %s, %s, %s, %s)
#                 """, (guest_id, hotel_id, rating, comment, review_date))
#         timed_block("Wstawianie recenzji", insert_reviews)
#
#         department_ids = []
#         def insert_departments():
#             for dep in ['Housekeeping', 'Reception', 'Management', 'Kitchen']:
#                 desc = fake.text(max_nb_chars=100)
#                 cursor.execute("""
#                     INSERT INTO departments (name, description)
#                     VALUES (%s, %s) RETURNING department_id
#                 """, (dep, desc))
#                 department_ids.append(cursor.fetchone()[0])
#         timed_block("Wstawianie działów", insert_departments)
#
#         employee_ids = []
#         def insert_employees():
#             for _ in range(10):
#                 hotel_id = random.choice(hotel_ids)
#                 first = fake.first_name()
#                 last = fake.last_name()
#                 position = random.choice(['Manager', 'Receptionist', 'Chef', 'Cleaner'])
#                 salary = round(random.uniform(1500, 5000), 2)
#                 department_id = random.choice(department_ids)
#                 cursor.execute("""
#                     INSERT INTO employees (hotel_id, first_name, last_name, position, salary, department_id)
#                     VALUES (%s, %s, %s, %s, %s, %s) RETURNING employee_id
#                 """, (hotel_id, first, last, position, salary, department_id))
#                 employee_ids.append(cursor.fetchone()[0])
#         timed_block("Wstawianie pracowników", insert_employees)
#
#         def insert_shifts():
#             for employee_id in employee_ids:
#                 for _ in range(2):
#                     shift_date = fake.date_between(start_date='today', end_date='+7d')
#                     start_hour = random.randint(6, 14)
#                     shift_start = f"{start_hour:02d}:00:00"
#                     shift_end = f"{(start_hour+8)%24:02d}:00:00"
#                     shift_type = random.choice(['morning', 'evening', 'night'])
#                     status = random.choice(['confirmed', 'cancelled'])
#                     department_id = random.choice(department_ids)
#                     cursor.execute("""
#                         INSERT INTO shift_schedules (employee_id, shift_date, shift_start, shift_end, shift_type, status, department_id)
#                         VALUES (%s, %s, %s, %s, %s, %s, %s)
#                     """, (employee_id, shift_date, shift_start, shift_end, shift_type, status, department_id))
#         timed_block("Wstawianie grafików", insert_shifts)
#
#         service_ids = []
#         def insert_services():
#             for name in ['Spa', 'Room Service', 'Laundry', 'Breakfast']:
#                 price = round(random.uniform(10, 100), 2)
#                 cursor.execute("""
#                     INSERT INTO services (name, price)
#                     VALUES (%s, %s) RETURNING service_id
#                 """, (name, price))
#                 service_ids.append(cursor.fetchone()[0])
#         timed_block("Wstawianie usług", insert_services)
#
#         def insert_services_used():
#             for _ in range(10):
#                 reservation_id = random.choice(reservation_ids)
#                 service_id = random.choice(service_ids)
#                 quantity = random.randint(1, 3)
#                 cursor.execute("""
#                     INSERT INTO services_used (reservation_id, service_id, quantity)
#                     VALUES (%s, %s, %s)
#                 """, (reservation_id, service_id, quantity))
#         timed_block("Wstawianie użytych usług", insert_services_used)
#
#         conn.commit()
#         print("\n✅ Wszystkie dane zostały wstawione pomyślnie.")
#         cursor.close()
#         conn.close()
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#
# insert_data()
