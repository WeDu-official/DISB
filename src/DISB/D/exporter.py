import sqlite3
import csv

conn = sqlite3.connect("miku8.db")
cursor = conn.cursor()

cursor.execute("SELECT * FROM file_metadata_table")
rows = cursor.fetchall()

with open("file_metadata_table2.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([desc[0] for desc in cursor.description])
    writer.writerows(rows)

conn.close()
