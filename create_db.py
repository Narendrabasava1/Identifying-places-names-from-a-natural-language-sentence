import mysql.connector
import pandas as pd

# Update with your MySQL credentials
DB_CONFIG = {
    "host": "localhost",
    "user": "root",            # your MySQL username
    "password": "root" # your MySQL password
}

DB_NAME = "geospatialdb"

def create_database():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    print(f"✅ Database `{DB_NAME}` ready.")
    cursor.close()
    conn.close()

def create_tables():
    conn = mysql.connector.connect(database=DB_NAME, **DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS Cities")
    cursor.execute("DROP TABLE IF EXISTS States")
    cursor.execute("DROP TABLE IF EXISTS Countries")

    cursor.execute("""
        CREATE TABLE Countries (
            id INT PRIMARY KEY,
            code VARCHAR(10),
            name VARCHAR(255) UNIQUE,
            phonecode VARCHAR(10)
        )
    """)
    cursor.execute("""
        CREATE TABLE States (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            country_id INT,
            FOREIGN KEY (country_id) REFERENCES Countries(id)
        )
    """)
    cursor.execute("""
        CREATE TABLE Cities (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            state_id INT,
            FOREIGN KEY (state_id) REFERENCES States(id)
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Tables created.")

def insert_from_csv():
    conn = mysql.connector.connect(database=DB_NAME, **DB_CONFIG)
    cursor = conn.cursor()

    # Read CSVs and replace NaN with None
    countries_df = pd.read_csv("countries.csv", header=None, names=["id", "code", "name", "phonecode"])
    states_df = pd.read_csv("states.csv", header=None, names=["id", "name", "country_id"])
    cities_df = pd.read_csv("cities.csv", header=None, names=["id", "name", "state_id"])

    countries_df = countries_df.where(pd.notnull(countries_df), None)
    states_df = states_df.where(pd.notnull(states_df), None)
    cities_df = cities_df.where(pd.notnull(cities_df), None)

    # Insert countries
    for _, row in countries_df.iterrows():
        cursor.execute(
            "INSERT IGNORE INTO Countries (id, code, name, phonecode) VALUES (%s, %s, %s, %s)",
            (row['id'], row['code'], row['name'], row['phonecode'])
        )

    # Insert states
    for _, row in states_df.iterrows():
        cursor.execute(
            "INSERT IGNORE INTO States (id, name, country_id) VALUES (%s, %s, %s)",
            (row['id'], row['name'], row['country_id'])
        )

    # Insert cities
    for _, row in cities_df.iterrows():
        cursor.execute(
            "INSERT IGNORE INTO Cities (id, name, state_id) VALUES (%s, %s, %s)",
            (row['id'], row['name'], row['state_id'])
        )

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Data inserted from CSV files (NULLs handled).")

if __name__ == "__main__":
    create_database()
    create_tables()
    insert_from_csv()
