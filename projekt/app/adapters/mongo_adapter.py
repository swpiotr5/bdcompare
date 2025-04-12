# adapters/mongo_adapter.py
from pymongo import MongoClient
from datetime import datetime
from bson.regex import Regex
import time

class MongoAdapter:
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27018/")
        self.db = self.client["hotel_management"]

    def query(self, name):
        start = time.time()
        results = []

        match name:
            # --- COMMON TESTS ---
            case "hotels_in_city":
                results = list(self.db.hotels.find({"city": "Warsaw"}).limit(10))

            case "available_rooms":
                results = list(self.db.rooms.find({"status": "available"}).limit(10))

            case "suite_rooms_above_300":
                results = list(self.db.room_details.find({"room_type": "suite", "price": {"$gt": 300}}).limit(10))

            case "confirmed_reservations":
                results = list(self.db.reservations.find({
                    "status": "confirmed",
                    "check_in": {"$gte": "2024-01-01", "$lte": "2024-12-31"}
                }).limit(10))

            case "reviews_5stars_recent":
                results = list(self.db.reviews.find({"rating": 5}).sort("review_date", -1).limit(10))

            case "high_salary_employees":
                results = list(self.db.employees.find({"salary": {"$gt": 9000}}).sort("last_name", 1).limit(10))

            case "expensive_reservations":
                results = list(self.db.reservations.find({"total_price": {"$gt": 2000}}).sort("total_price", -1).limit(10))

            case "gmail_guests":
                results = list(self.db.guests.find({"email": Regex("@gmail")}).limit(10))

            case "paypal_paid_payments":
                results = list(self.db.payments.find({"method": "paypal", "status": "paid"}).limit(10))

            case "avg_price_per_room_type":
                results = list(self.db.room_details.aggregate([
                    {"$group": {"_id": "$room_type", "avg_price": {"$avg": "$price"}}}
                ]))

            # --- ADVANCED TESTS ---
            case "monthly_reservation_count":
                results = list(self.db.reservations.aggregate([
                    {"$match": {"check_in": {"$gte": "2024-01-01", "$lte": "2024-12-31"}}},
                    {"$project": {
                        "month": {"$substr": ["$check_in", 0, 7]}
                    }},
                    {"$group": {"_id": "$month", "count": {"$sum": 1}}},
                    {"$sort": {"_id": 1}}
                ]))

            case "top_guests_total_spent":
                results = list(self.db.payments.aggregate([
                    {"$match": {"status": "paid"}},
                    {"$lookup": {
                        "from": "reservations",
                        "localField": "reservation_id",
                        "foreignField": "_id",
                        "as": "reservation"
                    }},
                    {"$unwind": "$reservation"},
                    {"$group": {
                        "_id": "$reservation.guest_id",
                        "total_spent": {"$sum": "$amount"}
                    }},
                    {"$sort": {"total_spent": -1}},
                    {"$limit": 5}
                ]))

            case "top_hotels_by_reviews":
                results = list(self.db.reviews.aggregate([
                    {"$group": {"_id": "$hotel_id", "review_count": {"$sum": 1}}},
                    {"$sort": {"review_count": -1}},
                    {"$limit": 5}
                ]))

            case "employees_above_dept_avg":
                results = list(self.db.employees.aggregate([
                    {"$group": {"_id": "$department_id", "avg_salary": {"$avg": "$salary"}}},
                    {"$lookup": {
                        "from": "employees",
                        "localField": "_id",
                        "foreignField": "department_id",
                        "as": "employees"
                    }},
                    {"$unwind": "$employees"},
                    {"$match": {"$expr": {"$gt": ["$employees.salary", "$avg_salary"]}}},
                    {"$replaceRoot": {"newRoot": "$employees"}},
                    {"$limit": 10}
                ]))

            case "room_never_reserved":
                results = list(self.db.rooms.aggregate([
                    {"$lookup": {
                        "from": "reservations",
                        "localField": "_id",
                        "foreignField": "room_id",
                        "as": "res"
                    }},
                    {"$match": {"res": {"$size": 0}}},
                    {"$limit": 10}
                ]))

            case _:
                raise ValueError(f"Unknown query name: {name}")

        duration = time.time() - start
        return {
            "query_name": name,
            "duration": duration,
            "row_count": len(results),
            "results": results[:5]
        }
