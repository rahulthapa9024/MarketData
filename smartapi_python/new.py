import sqlite3
import time

# File paths
main_db = "market_data.db"
source_db = "market_data1.db"
table_name = "market_data"  # Use your actual table name

# Connect to main database
conn = sqlite3.connect(main_db)
cursor = conn.cursor()

# Attach the source database
cursor.execute(f"ATTACH DATABASE '{source_db}' AS src")

# Insert all rows from source to main
cursor.execute(f"""
    INSERT INTO {table_name} (symbol, date, buy_sell_volume_percent)
    SELECT symbol, date, buy_sell_volume_percent FROM src.{table_name}
""")

# 💡 Commit to finalize changes
conn.commit()

# ⏱ Optional: short delay before detaching (helps release write lock)
time.sleep(0.5)

# Detach the source database
cursor.execute("DETACH DATABASE src")

# Final cleanup
conn.commit()
conn.close()

print("✅ All rows merged from market_data1.db into market_data.db")
