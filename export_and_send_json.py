import json
import time
import mysql.connector
import websocket
from datetime import date, datetime

def send_data():
    while True:  # Infinite loop to keep reconnecting
        try:
            print("🔄 Connecting to WebSocket server...")
            ws = websocket.create_connection("wss://wsserver-production-afea.up.railway.app")  # Connect to WebSocket
            print("✅ Connected to WebSocket server.")

            while True:  # Keep sending data unless an error occurs
                try:
                    conn = mysql.connector.connect(
                        host="127.0.0.1",
                        user="root",
                        password="",
                        database="wilms-server_db",
                        charset="utf8mb4",
                        collation="utf8mb4_general_ci"
                    )
                    cursor = conn.cursor()

                    # 🔹 Query 1: Get ongoing/booked bookings
                    booking_query = """
                        SELECT f.id AS facilityID, b.date, 
                            TIME_FORMAT(b.startTime, '%H:%i:%s') AS start, 
                            TIME_FORMAT(b.endTime, '%H:%i:%s') AS end, 
                            f.facilityName, b.description AS title, 
                            b.headcount, b.isFacilityBooked
                        FROM api_booking AS b 
                        JOIN facility_facility AS f ON b.venue_id = f.id 
                        WHERE (b.status = 'BOOKED' OR b.status = 'ONGOING') 
                        AND DATE(b.date) >= CURDATE() 
                        ORDER BY b.date ASC, b.startTime ASC
                    """
                    cursor.execute(booking_query)
                    booking_rows = cursor.fetchall()
                    booking_columns = [desc[0] for desc in cursor.description]
                    bookings_json = [dict(zip(booking_columns, row)) for row in booking_rows]

                    # 🔹 Query 2: Get all facilities
                    facility_query = """
                        SELECT id AS facilityID, facilityname, capacity, is_conference FROM facility_facility WHERE isdeleted = 0
                    """
                    cursor.execute(facility_query)
                    facility_rows = cursor.fetchall()
                    facility_columns = [desc[0] for desc in cursor.description]
                    facilities_json = [dict(zip(facility_columns, row)) for row in facility_rows]

                    # 🔹 Combine both results into a final JSON structure
                    final_json = {
                        "bookings": bookings_json,
                        "facilities": facilities_json
                    }

                    json_data = json.dumps(final_json, indent=4, default=str)  # Convert to JSON

                    ws.send(json_data)  # Send data through WebSocket
                    print(f"✅ Sent data to WebSocket server on {datetime.now()}")

                    cursor.close()
                    conn.close()

                except mysql.connector.Error as db_err:
                    print("❌ Database Error:", db_err)
                except Exception as e:
                    print("❌ Unexpected Error while sending data:", e)
                    break  # Break inner loop and retry WebSocket connection

                time.sleep(30)  # Wait before sending the next batch

        except Exception as e:
            print("❌ WebSocket Connection Error:", e)
            print("🔄 Retrying WebSocket connection in 5 seconds...")
            time.sleep(5)  # Wait before reconnecting

if __name__ == "__main__":
    send_data()
