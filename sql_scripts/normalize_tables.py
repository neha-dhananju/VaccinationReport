import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------
# 1. DB Connection
# -------------------------
DB_USER = "root"
DB_PASSWORD = "root1234"
DB_HOST = "localhost"
DB_NAME = "immunization_db"

engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# -------------------------
# 2. Load Data
# -------------------------
coverage_df = pd.read_csv("../cleaned/coverage_data_cleaned.csv")
intro_df = pd.read_csv("../cleaned/vaccine_intro_cleaned.csv")
schedule_df = pd.read_csv("../cleaned/vaccine_schedule_cleaned.csv")
incidence_df = pd.read_csv("../cleaned/incidence_rate_cleaned.csv")
reported_df = pd.read_csv("../cleaned/reported_cases_cleaned.csv")

# -------------------------
# 3. Create Master Tables
# -------------------------
with engine.connect() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS countries (
        iso_3_code VARCHAR(5) PRIMARY KEY,
        country_name VARCHAR(150),
        who_region VARCHAR(50)
    );
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS vaccines (
        vaccine_code VARCHAR(50) PRIMARY KEY,
        vaccine_description TEXT
    );
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS diseases (
        disease_code VARCHAR(50) PRIMARY KEY,
        disease_description TEXT
    );
    """))

print("✅ Master tables created!")

# -------------------------
# 4. Insert Data into Master Tables
# -------------------------
# Countries
coverage_countries = coverage_df[['CODE', 'NAME', 'GROUP']].rename(
    columns={"CODE": "iso_3_code", "NAME": "country_name", "GROUP": "who_region"}
)
intro_countries = intro_df[['ISO_3_CODE', 'COUNTRYNAME', 'WHO_REGION']].rename(
    columns={"ISO_3_CODE": "iso_3_code", "COUNTRYNAME": "country_name", "WHO_REGION": "who_region"}
)
schedule_countries = schedule_df[['iso_3_code', 'countryname', 'who_region']].rename(
    columns={"countryname": "country_name"}
)
countries_df = pd.concat([coverage_countries, intro_countries, schedule_countries]).drop_duplicates(subset=['iso_3_code'])
countries_df.to_sql("countries", con=engine, if_exists="replace", index=False)

# Vaccines
vaccines_df = schedule_df[['vaccinecode', 'vaccine_description']].drop_duplicates()
vaccines_df = vaccines_df.rename(columns={"vaccinecode": "vaccine_code"})
vaccines_df.to_sql("vaccines", con=engine, if_exists="replace", index=False)

# Diseases
diseases_df = incidence_df[['DISEASE', 'DISEASE_DESCRIPTION']].drop_duplicates()
diseases_df = diseases_df.rename(columns={"DISEASE": "disease_code", "DISEASE_DESCRIPTION": "disease_description"})
diseases_df.to_sql("diseases", con=engine, if_exists="replace", index=False)

print("✅ Master data inserted!")

# -------------------------
# 5. Create Fact Tables With Foreign Keys
# -------------------------
with engine.connect() as conn:
    # Coverage
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS coverage_normalized (
        id INT AUTO_INCREMENT PRIMARY KEY,
        iso_3_code VARCHAR(5),
        vaccine_code VARCHAR(50),
        year INT,
        coverage FLOAT,
        FOREIGN KEY (iso_3_code) REFERENCES countries(iso_3_code),
        FOREIGN KEY (vaccine_code) REFERENCES vaccines(vaccine_code)
    );
    """))

    # Vaccine Schedule
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS vaccine_schedule_normalized (
        id INT AUTO_INCREMENT PRIMARY KEY,
        iso_3_code VARCHAR(5),
        vaccine_code VARCHAR(50),
        year INT,
        schedulerounds VARCHAR(50),
        ageadministered VARCHAR(50),
        sourcecomment TEXT,
        FOREIGN KEY (iso_3_code) REFERENCES countries(iso_3_code),
        FOREIGN KEY (vaccine_code) REFERENCES vaccines(vaccine_code)
    );
    """))

    # Vaccine Intro
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS vaccine_intro_normalized (
        id INT AUTO_INCREMENT PRIMARY KEY,
        iso_3_code VARCHAR(5),
        vaccine_status VARCHAR(50),
        year INT,
        FOREIGN KEY (iso_3_code) REFERENCES countries(iso_3_code)
    );
    """))

print("✅ Fact tables created with foreign keys!")

print("\n🎉 Database normalization + data integrity setup completed successfully!")
