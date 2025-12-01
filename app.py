from flask import Flask, render_template, request
import mysql.connector
import re
from rapidfuzz import fuzz

app = Flask(__name__)

# Function to connect to MySQL
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",       # or your MySQL server IP
        user="root",            # your MySQL username
        password="root",# your MySQL password
        database="geospatialdb" # the DB you created
    )

import re
from rapidfuzz import fuzz
import unicodedata

def normalize_text(text):
    """Lowercase + strip accents for robust matching"""
    text = text.lower().strip()
    text = ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )
    return text

def find_and_map_place_names(sentence, table_name, cursor):
    canonical_names = []
    results_seen = set()

    sentence_norm = normalize_text(sentence)

    # Fetch all rows from DB
    cursor.execute(f"SELECT name FROM {table_name}")
    rows = [row[0] for row in cursor.fetchall()]
    rows = [name for name in rows if name]

    for name in rows:
        name_norm = normalize_text(name)

        # ✅ Regex whole-word / phrase match
        pattern = r"\b" + re.escape(name_norm) + r"\b"
        if re.search(pattern, sentence_norm):
            key = (name_norm, table_name)
            if key not in results_seen:
                results_seen.add(key)
                canonical_names.append({
                    "token": name,
                    "canonical_name": name,
                    "table": table_name
                })
            continue

        # ✅ Fuzzy match fallback (only longer names, reduce noise)
        if len(name_norm) > 3:
            similarity = fuzz.ratio(name_norm, sentence_norm)
            if similarity > 90:   # stricter threshold
                key = (name_norm, table_name)
                if key not in results_seen:
                    results_seen.add(key)
                    canonical_names.append({
                        "token": name,
                        "canonical_name": name,
                        "table": table_name
                    })

    return canonical_names

@app.route("/", methods=["GET", "POST"])
def home():
    results = []
    sentence = ""
    if request.method == "POST":
        sentence = request.form.get("sentence", "")
        conn = get_db_connection()
        cursor = conn.cursor()
        for table in ['Countries', 'States', 'Cities']:
            results.extend(find_and_map_place_names(sentence, table, cursor))
        cursor.close()
        conn.close()

    return render_template("index.html", results=results, sentence=sentence)


if __name__ == "__main__":
    app.run(debug=True)
