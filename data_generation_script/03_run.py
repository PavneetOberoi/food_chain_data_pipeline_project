import os
import importlib


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    data_dir = os.path.join(project_root, "synthetic_data")
    os.makedirs(data_dir, exist_ok=True)

    sql_db = importlib.import_module("00_sql_db")
    sql_db.generate_data_for_sql_db()

    historical_orders = importlib.import_module("01_historical_orders")
    historical_orders.generate_historical_orders()

    reviews = importlib.import_module("02_reviews")
    reviews.generate_customer_reviews(review_percentage=0.01)
