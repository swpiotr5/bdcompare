# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
from adapters.mongo_adapter import MongoAdapter
from adapters.cassandra_adapter import CassandraAdapter
from adapters.postgre_adapter import PostgresAdapter
from adapters.mysql_adapter import MySQLAdapter

# -------------------- CONFIG --------------------
st.set_page_config(page_title="DB Benchmark GUI", layout="wide")
st.markdown("""
    <style>
        .stApp { background-color: #0e1117; color: white; }
        .css-1v3fvcr { color: white; }
        .stSelectbox > div > div { color: black; }
    </style>
""", unsafe_allow_html=True)

# -------------------- TITLE --------------------
st.title("🚀 Interaktywny Benchmark Baz Danych")

# -------------------- SETUP --------------------
ADAPTERS = {
    "MongoDB": MongoAdapter(),
    "Cassandra": CassandraAdapter(),
    "MySQL": MySQLAdapter(),
    "PostgreSQL": PostgresAdapter(),
}

TESTS = [
    "hotels_in_city",
    "available_rooms",
    "suite_rooms_above_300",
    "confirmed_reservations",
    "reviews_5stars_recent",
    "high_salary_employees",
    "expensive_reservations",
    "gmail_guests",
    "paypal_paid_payments",
    "avg_price_per_room_type",
    "monthly_reservation_count",
    "top_guests_total_spent",
    "top_hotels_by_reviews",
    "employees_above_dept_avg",
    "room_never_reserved"
]

# -------------------- SIDEBAR --------------------
st.sidebar.header("⚙️ Ustawienia")
selected_dbs = st.sidebar.multiselect("Wybierz bazy danych:", list(ADAPTERS.keys()), default=["MongoDB"])
selected_tests = st.sidebar.multiselect("Wybierz testy:", TESTS, default=["hotels_in_city"])
run_button = st.sidebar.button("🔍 Uruchom testy")

# -------------------- RUN BENCHMARK --------------------
if run_button and selected_dbs and selected_tests:
    st.subheader("📊 Wyniki Benchmarku")

    all_results = []

    for db_name in selected_dbs:
        adapter = ADAPTERS[db_name]
        for test in selected_tests:
            with st.spinner(f"{db_name} → {test}..."):
                try:
                    result = adapter.query(test)
                    all_results.append({
                        "Baza": db_name,
                        "Test": test,
                        "Czas [s]": round(result["duration"], 3),
                        "Wyniki": result["row_count"],
                        "Sample": result["results"]
                    })
                except Exception as e:
                    all_results.append({
                        "Baza": db_name,
                        "Test": test,
                        "Czas [s]": -1,      # ✅
                        "Wyniki": -1,        # ✅ zawsze int!
                        "Sample": str(e)
                    })


    # Exclude 'Sample' from the main dataframe to avoid pyarrow.ObjectId issues
    df = pd.DataFrame([{k: v for k, v in row.items() if k != "Sample"} for row in all_results])
    st.dataframe(df, use_container_width=True)

    # -------------------- PLOT --------------------
    chart = px.bar(df, x="Test", y="Czas [s]", color="Baza", barmode="group",
                   title="⏱️ Porównanie czasu wykonania")
    st.plotly_chart(chart, use_container_width=True)

    # -------------------- OPTIONAL DETAILS --------------------
    if st.checkbox("🔍 Pokaż przykładowe wyniki zapytań"):
        for row in all_results:
            st.markdown(f"### 🧪 {row['Baza']} – {row['Test']}")
            st.json(row['Sample'])
