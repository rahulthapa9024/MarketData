import sqlite3

# --- CONFIG ---
DB_PATH = "market_data.db"
TABLE_NAME = "market_data"
START_ROW = 174   # Inclusive
END_ROW = 418  # Inclusive

# --- Connect to DB ---
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

try:
    # Step 1: Get rowids of rows in desired range
    cursor.execute(f"""
        SELECT rowid FROM {TABLE_NAME}
        ORDER BY rowid
        LIMIT ? OFFSET ?
    """, (END_ROW - START_ROW + 1, START_ROW - 1))
    
    rowids = [row[0] for row in cursor.fetchall()]

    if not rowids:
        print(f"⚠️ No rows found between row {START_ROW} and {END_ROW}")
    else:
        # Step 2: Delete by rowid
        cursor.executemany(f"DELETE FROM {TABLE_NAME} WHERE rowid = ?", [(rid,) for rid in rowids])
        conn.commit()
        print(f"✅ Deleted {cursor.rowcount} rows (row numbers {START_ROW} to {END_ROW})")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    conn.close()
