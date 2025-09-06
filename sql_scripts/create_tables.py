import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text

# ---------------------------------------------------------
# STEP 1: Database Connection Setup
# ---------------------------------------------------------
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "root1234"
DB_NAME = "immunization_db"

# Create SQLAlchemy engine
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# ---------------------------------------------------------
# STEP 2: CSV File Paths
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
# STEP 3: Create Tables with Correct Data Types
# ---------------------------------------------------------
create_table_queries = {
    "coverage": """
        CREATE TABLE IF NOT EXISTS coverage (
            group_name VARCHAR(50),
            iso_3_code VARCHAR(3),
            country_name VARCHAR(255),
            year INT,
            antigen VARCHAR(50),
            antigen_description VARCHAR(255),
            coverage_category VARCHAR(100),
            coverage_category_desc TEXT,
            target_number INT,
            doses INT,
            coverage FLOAT,
            coverage_status VARCHAR(50),
            dose_gap VARCHAR(50),
            vaccine_efficiency VARCHAR(50),
            recent_data VARCHAR(10)
        )
    """,
    "incidence_rate": """
        CREATE TABLE IF NOT EXISTS incidence_rate (
            group_name VARCHAR(50),
            iso_3_code VARCHAR(3),
            country_name VARCHAR(255),
            year INT,
            disease VARCHAR(50),
            disease_description VARCHAR(255),
            denominator VARCHAR(50),
            incidence_rate FLOAT,
            denominator_value FLOAT,
            incidence_severity VARCHAR(50),
            recent_data VARCHAR(10)
        )
    """,
    "reported_cases": """
        CREATE TABLE IF NOT EXISTS reported_cases (
            group_name VARCHAR(50),
            iso_3_code VARCHAR(3),
            country_name VARCHAR(255),
            year INT,
            disease VARCHAR(50),
            disease_description VARCHAR(255),
            cases INT,
            case_severity VARCHAR(50),
            recent_data VARCHAR(10)
        )
    """,
    "vaccine_intro": """
        CREATE TABLE IF NOT EXISTS vaccine_intro (
            iso_3_code VARCHAR(3),
            country_name VARCHAR(255),
            who_region VARCHAR(50),
            year INT,
            description TEXT,
            intro VARCHAR(10),
            recently_introduced VARCHAR(10),
            total_vaccines_intro INT,
            vaccine_status VARCHAR(50)
        )
    """,
    "vaccine_schedule": """
        CREATE TABLE IF NOT EXISTS vaccine_schedule (
        iso_3_code VARCHAR(3),
        country_name VARCHAR(255),
        who_region VARCHAR(50),
        year INT,
        vaccine_code VARCHAR(20),
        vaccine_description VARCHAR(255),
        schedule_rounds VARCHAR(50),
        target_pop VARCHAR(100),
        target_pop_desc TEXT,
        geo_area VARCHAR(255),
        age_administered VARCHAR(50),
        source_comment LONGTEXT
    )
"""

    
}

with engine.connect() as connection:
    for table, query in create_table_queries.items():
        connection.execute(text(query))
    print("✅ Tables created successfully!")

# ---------------------------------------------------------
# STEP 4: Insert Data from CSVs
# ---------------------------------------------------------
for table_name, filename in datasets.items():
    file_path = os.path.join(cleaned_path, filename)

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        continue

    print(f"\n📥 Inserting data into '{table_name}'...")
    df = pd.read_csv(file_path)

    # Insert into MySQL
    df.to_sql(table_name, con=engine, if_exists="replace", index=False, 
              dtype={
                    **{col: sqlalchemy.types.VARCHAR(255) 
                       for col in df.columns 
                       if col.lower() != "sourcecomment"
        },
        "sourcecomment": sqlalchemy.types.Text().with_variant(sqlalchemy.dialects.mysql.LONGTEXT(), "mysql") # Use TEXT explicitly
       
    }
    )

    print(f"✅ Data inserted into '{table_name}' successfully!")

print("\n🎉 All tables created and data inserted successfully!")
