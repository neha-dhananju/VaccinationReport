import os
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# ---------------------------------------------------------
# STEP 1: Database Connection Setup
# ---------------------------------------------------------
DB_HOST = "localhost"         
DB_USER = "root"              #  MySQL username
DB_PASSWORD = "root1234"      #  MySQL password
DB_NAME = "immunization_db"   # Database name

# Create database if it doesn't exist
conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD)
cursor = conn.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
conn.commit()
cursor.close()
conn.close()

# Create SQLAlchemy engine for easier integration
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# ---------------------------------------------------------
# STEP 2: Load Cleaned CSV Files
# ---------------------------------------------------------
cleaned_path = "../cleaned"

datasets = {
    "coverage": "coverage_data_cleaned.csv",
    "incidence_rate": "incidence_rate_cleaned.csv",
    "reported_cases": "reported_cases_cleaned.csv",
    "vaccine_intro": "vaccine_intro_cleaned.csv",
    "vaccine_schedule": "vaccine_schedule_cleaned.csv"
}

# ---------------------------------------------------------
# STEP 3: Create Tables If Not Exists & Insert Data
# ---------------------------------------------------------
for table_name, filename in datasets.items():
    file_path = os.path.join(cleaned_path, filename)
    
    if not os.path.exists(file_path):
        print(f" File not found: {file_path}")
        continue

    print(f"\n📂 Processing: {table_name} → {filename}")

    # Load CSV into DataFrame
    df = pd.read_csv(file_path)

    # Store DataFrame into SQL table (create if not exists)
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)

    print(f" Table '{table_name}' created & data inserted successfully!")

print("\n All tables created successfully in the database!")

# ---------------------------------------------------------
# STEP 4: Verify Table Creation
# ---------------------------------------------------------
with engine.connect() as connection:
    result = connection.execute("SHOW TABLES")
    print("\nTables in database:")
    for row in result:
        print(" -", row[0])
