# adapters/cassandra_adapter.py
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from collections import Counter, defaultdict
from datetime import datetime
import time

class CassandraAdapter:
    def __init__(self):
        auth = PlainTextAuthProvider(username="cassandra", password="cassandra")
        cluster = Cluster(["localhost"], port=9044, auth_provider=auth)
        self.session = cluster.connect("hotel_management_3")

    def query(self, name):
        start = time.time()
        results = []

        match name:
            # --- COMMON TESTS ---
            case "hotels_in_city":
                results = self.session.execute("SELECT * FROM hotels WHERE city='Warsaw' ALLOW FILTERING").all()

            case "available_rooms":
                results = self.session.execute("SELECT * FROM rooms WHERE status='available' ALLOW FILTERING").all()

            case "suite_rooms_above_300":
                results = [row for row in self.session.execute("SELECT * FROM room_details WHERE room_type='suite' ALLOW FILTERING") if row.price > 300]

            case "confirmed_reservations":
                results = [r for r in self.session.execute("SELECT * FROM reservations WHERE status='confirmed' ALLOW FILTERING") if r.check_in.year == 2024]

            case "reviews_5stars_recent":
                results = sorted(self.session.execute("SELECT * FROM reviews WHERE rating=5 ALLOW FILTERING"), key=lambda r: r.review_date, reverse=True)[:10]

            case "high_salary_employees":
                results = sorted([e for e in self.session.execute("SELECT * FROM employees ALLOW FILTERING") if e.salary > 9000], key=lambda e: e.last_name)[:10]

            case "expensive_reservations":
                results = sorted([r for r in self.session.execute("SELECT * FROM reservations ALLOW FILTERING") if r.total_price > 2000], key=lambda r: r.total_price, reverse=True)[:10]

            case "gmail_guests":
                results = [g for g in self.session.execute("SELECT * FROM guests ALLOW FILTERING") if "@gmail" in g.email][:10]

            case "paypal_paid_payments":
                results = [p for p in self.session.execute("SELECT * FROM payments WHERE method='paypal' AND status='paid' ALLOW FILTERING")][:10]

            case "avg_price_per_room_type":
                data = defaultdict(list)
                for r in self.session.execute("SELECT * FROM room_details ALLOW FILTERING"):
                    data[r.room_type].append(r.price)
                results = [{"room_type": k, "avg_price": sum(v)/len(v)} for k, v in data.items() if v]

            # --- ADVANCED TESTS ---
            case "monthly_reservation_count":
                counts = Counter()
                for r in self.session.execute("SELECT * FROM reservations ALLOW FILTERING"):
                    if r.check_in.year == 2024:
                        key = f"{r.check_in.year}-{r.check_in.month:02}"
                        counts[key] += 1
                results = [{"month": k, "count": v} for k, v in sorted(counts.items())]

            case "top_guests_total_spent":
                totals = defaultdict(float)
                reservation_map = {}
                for r in self.session.execute("SELECT * FROM reservations ALLOW FILTERING"):
                    reservation_map[r.reservation_id] = r.guest_id
                for p in self.session.execute("SELECT * FROM payments WHERE status='paid' ALLOW FILTERING"):
                    guest_id = reservation_map.get(p.reservation_id)
                    if guest_id:
                        totals[guest_id] += p.amount
                results = sorted(({"guest_id": k, "total": v} for k, v in totals.items()), key=lambda x: x["total"], reverse=True)[:5]

            case "top_hotels_by_reviews":
                counts = Counter(r.hotel_id for r in self.session.execute("SELECT * FROM reviews ALLOW FILTERING"))
                results = sorted(({"hotel_id": k, "count": v} for k, v in counts.items()), key=lambda x: x["count"], reverse=True)[:5]

            case "employees_above_dept_avg":
                dept_data = defaultdict(list)
                for e in self.session.execute("SELECT * FROM employees ALLOW FILTERING"):
                    dept_data[e.department_id].append(e)
                result = []
                for dept_id, employees in dept_data.items():
                    avg = sum(e.salary for e in employees) / len(employees)
                    result.extend([e for e in employees if e.salary > avg])
                results = result[:10]

            case "room_never_reserved":
                reserved_ids = set(r.room_id for r in self.session.execute("SELECT room_id FROM reservations ALLOW FILTERING"))
                results = [r for r in self.session.execute("SELECT * FROM rooms ALLOW FILTERING") if r.room_id not in reserved_ids][:10]

            case _:
                raise ValueError(f"Unknown query name: {name}")

        duration = time.time() - start
        return {
            "query_name": name,
            "duration": duration,
            "row_count": len(results),
            "results": results[:5]  # przykładowe wyniki
        }
