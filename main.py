
import pyodbc

DRIVER="{ODBC Driver 18 for SQL Server}"
SERVER="tcp:weatherappp.database.windows.net"
DATABASE="free-sql-db-9236210;"
UID= "localhost"
PWD="A@p8103101921"

conn = pyodbc.connect(f'driver={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD}')


cursor = conn.cursor()

cursor.execute("""

CREATE TABLE weather_data (
    id INT IDENTITY(1,1) PRIMARY KEY,
    city NVARCHAR(100),
    temperature FLOAT,
    temperature_min FLOAT,
    temperature_max FLOAT,
    humidity INT,
    sunrise DATETIME,
    sunset DATETIME,
    wind_speed FLOAT,
    recorded_at DATETIME
)
""")

conn.commit()
print("Table created!")

cursor.close()
conn.close()
