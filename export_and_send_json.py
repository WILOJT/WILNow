import pandas as pd
import mysql.connector
import requests
import time

while True:
    try:
        # 1️⃣ Establish database connection
        conn = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="",
            database="asd",
            charset="utf8mb4",
            collation="utf8mb4_general_ci"
        )

        # 2️⃣ Create a cursor
        cursor = conn.cursor()

        # 3️⃣ Write your SQL query
        query = ("""
            SELECT
                f.id AS facilityID,
                b.date,
                TIME_FORMAT(b.startTime, '%H:%i:%s') AS startTime,
                TIME_FORMAT(b.endTime, '%H:%i:%s') AS endTime,
                f.facilityname,
                b.description,
                b.headcount,
                b.isFacilityBooked,
                f.capacity
            FROM api_booking AS b
            JOIN facility_facility AS f
            ON b.venue_id = f.id
            WHERE b.status = 'BOOKED' OR b.status = 'ONGOING'
            ORDER BY b.date ASC, b.startTime ASC
        """)

        # 4️⃣ Execute query and fetch results
        cursor.execute(query)
        rows = cursor.fetchall()

        # 5️⃣ Get column names
        columns = [desc[0] for desc in cursor.description]

        # 6️⃣ Read data into a Pandas DataFrame
        df = pd.DataFrame(rows, columns=columns)

        # 7️⃣ Convert DataFrame to JSON
        json_data = df.to_json(orient="records", indent=4)
        print(json_data)

        # 8️⃣ Send JSON data to Vercel API
        url = "https://your-vercel-app.vercel.app/api/updateData"  # Update with your Vercel API URL
        data = {"data": json_data}  # Append actual JSON data

        try:
            response = requests.post(url, json=data, headers={"Content-Type": "application/json"})
            print("Response:", response.json())
        except requests.exceptions.RequestException as e:
            print("Error sending data:", e)

        # 9️⃣ Close the database connection
        cursor.close()
        conn.close()

    except mysql.connector.Error as db_err:
        print("Database Error:", db_err)
    except Exception as e:
        print("Unexpected Error:", e)

    #  🔄 Wait for 5 minutes before running again
    time.sleep(300)
