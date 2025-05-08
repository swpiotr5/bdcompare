# adapters/postgres_adapter.py
import psycopg2
import time
import traceback

class PostgresAdapter:
    def __init__(self):
        self.conn = None

    def connect(self):
        if self.conn is None or self.conn.closed:
            try:
                self.conn = psycopg2.connect(
                    host="localhost",
                    port="5434",
                    dbname="mydb",
                    user="user",
                    password="password"
                )
            except Exception as e:
                # Rozwiązanie 1: bezpieczny komunikat
                print("Błąd połączenia z PostgreSQL:", repr(e))
                # Rozwiązanie 2: jeśli koniecznie chcesz widzieć tekst błędu
                # print("Błąd połączenia:", str(e).encode("utf-8", errors="replace").decode("utf-8"))
                raise Exception("Nie udało się połączyć z PostgreSQL")


    def query(self, name):
        self.connect()
        start = time.time()

        try:
            with self.conn.cursor() as cursor:
                match name:
                    case "hotels_in_city":
                        cursor.execute("SELECT * FROM hotels WHERE city = 'Warsaw' LIMIT 10;")
                    case "available_rooms":
                        cursor.execute("SELECT * FROM rooms WHERE status = 'available' LIMIT 10;")
                    case "suite_rooms_above_300":
                        cursor.execute("SELECT * FROM room_details WHERE room_type = 'suite' AND price > 300 LIMIT 10;")
                    case "confirmed_reservations":
                        cursor.execute("""
                            SELECT * FROM reservations
                            WHERE status = 'confirmed'
                            AND check_in BETWEEN '2024-01-01' AND '2024-12-31'
                            LIMIT 10;
                        """)
                    case "reviews_5stars_recent":
                        cursor.execute("""
                            SELECT * FROM reviews
                            WHERE rating = 5
                            ORDER BY review_date DESC
                            LIMIT 10;
                        """)
                    case "high_salary_employees":
                        cursor.execute("""
                            SELECT * FROM employees
                            WHERE salary > 9000
                            ORDER BY last_name
                            LIMIT 10;
                        """)
                    case "expensive_reservations":
                        cursor.execute("""
                            SELECT * FROM reservations
                            WHERE total_price > 2000
                            ORDER BY total_price DESC
                            LIMIT 10;
                        """)
                    case "gmail_guests":
                        cursor.execute("""
                            SELECT * FROM guests
                            WHERE email LIKE '%@gmail%'
                            LIMIT 10;
                        """)
                    case "paypal_paid_payments":
                        cursor.execute("""
                            SELECT * FROM payments
                            WHERE method = 'paypal' AND status = 'paid'
                            LIMIT 10;
                        """)
                    case "avg_price_per_room_type":
                        cursor.execute("""
                            SELECT room_type, AVG(price) AS avg_price
                            FROM room_details
                            GROUP BY room_type;
                        """)
                    case "monthly_reservation_count":
                        cursor.execute("""
                            SELECT TO_CHAR(check_in, 'YYYY-MM') AS month, COUNT(*)
                            FROM reservations
                            WHERE check_in BETWEEN '2024-01-01' AND '2024-12-31'
                            GROUP BY month
                            ORDER BY month;
                        """)
                    case "top_guests_total_spent":
                        cursor.execute("""
                            SELECT r.guest_id, SUM(p.amount) AS total_spent
                            FROM payments p
                            JOIN reservations r ON p.reservation_id = r.reservation_id
                            WHERE p.status = 'paid'
                            GROUP BY r.guest_id
                            ORDER BY total_spent DESC
                            LIMIT 5;
                        """)
                    case "top_hotels_by_reviews":
                        cursor.execute("""
                            SELECT hotel_id, COUNT(*) AS review_count
                            FROM reviews
                            GROUP BY hotel_id
                            ORDER BY review_count DESC
                            LIMIT 5;
                        """)
                    case "employees_above_dept_avg":
                        cursor.execute("""
                            SELECT e.*
                            FROM employees e
                            JOIN (
                                SELECT department_id, AVG(salary) AS avg_salary
                                FROM employees
                                GROUP BY department_id
                            ) d ON e.department_id = d.department_id
                            WHERE e.salary > d.avg_salary
                            LIMIT 10;
                        """)
                    case "room_never_reserved":
                        cursor.execute("""
                            SELECT rooms.*
                            FROM rooms
                            LEFT JOIN reservations ON rooms.room_id = reservations.room_id
                            WHERE reservations.room_id IS NULL
                            LIMIT 10;
                        """)
                    case _:
                        raise ValueError(f"Unknown query name: {name}")

                results = cursor.fetchall()

        except Exception as e:
            print("Błąd podczas wykonania zapytania:", traceback.format_exc())
            raise

        duration = time.time() - start
        return {
            "query_name": name,
            "duration": duration,
            "row_count": len(results),
            "results": results[:5]  # dla przykładu tylko 5 wyników
        }
