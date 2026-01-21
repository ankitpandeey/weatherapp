import sqlite3
import json

with open("mp_only.json", "r", encoding="utf-8") as f:
 data = json.load(f)

con = sqlite3.Connection("weatherData.db")
cursor = con.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS cities (
    cityID INTEGER PRIMARY KEY,
    cityName TEXT NOT NULL,
    lat TEXT NOT NULL,
    lon TEXT NOT NULL
)
""")

def PopulateCitiesTable(id, cityName, lat,lan):
    cursor.execute("INSERT INTO cities VALUES(:cityID, :cityName, :lat, :lan)",{'cityID' : id,'cityName' : cityName, "lat":lat, "lan" :lan})
                
#for city in data:
 # PopulateCitiesTable(city["id"],city["name"],city["lat"],city["lon"])  

cursor.execute("Select * from cities")
print(cursor.fetchall()) 

con.commit()
con.close()