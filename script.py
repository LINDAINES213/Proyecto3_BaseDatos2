import json
import os
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Funciones para generar datos aleatorios
def random_datetime(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def random_flight_number():
    return fake.bothify(text='??####', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')

airport_codes = ["LAX", "JFK", "SFO", "ORD", "DFW", "ATL", "DEN", "LAS", "MIA", "SEA", "PHX", "CLT", "MCO", "EWR", "IAH", "MSP", "DTW", "PHL", "BOS", "LGA", "FLL", "BWI", "SLC", "SAN", "IAD", "DCA", "MDW", "TPA", "HNL", "PDX", "SMF", "MSY", "RDU", "MCI", "SAT", "CLE", "OAK", "BNA", "AUS", "SJC", "RSW", "IND", "PIT", "MKE", "CMH", "JAX", "BUF", "ANC", "HOU", "SNA", "ONT", "CVG", "ALB", "BHM", "OKC", "PBI", "OMA", "MEM", "BOI", "LIT", "SDF", "ORF", "TUL", "ROC", "GRR", "DSM", "BTV", "DAY", "RIC", "SYR", "MSN", "ICT", "TYS", "GSO", "CHA", "ELP", "PVD", "SAV", "ABQ", "PWM", "GSP", "CRW", "FAT", "LEX", "AMA", "MHT", "GEG", "BTR", "CAK", "XNA", "TVC", "SGF", "SBN", "AVL", "EVV", "FWA", "HSV", "MOB", "MLI", "PIA", "ABE", "AGS", "CID", "LBB", "BMI", "SBP", "MFR", "LAN", "FSD", "FWA", "GRB", "GPT", "BIL", "BZN", "SBN", "ABQ", "ELP", "CRW", "TOL", "FAR", "AZO", "DAB", "EUG", "SGU", "LGB", "RAP", "GJT", "SPI", "PSP", "SBA", "MRY", "HDN", "ACY", "JAN", "LEX", "PBG", "PSC", "EGE", "IDA", "ILM", "FCA", "FLG", "GTF", "BIS", "MOT", "ROW", "RAP", "ITH", "PLN", "ESC", "MQT", "CMX", "OWB", "ACK", "MVY", "HYA", "WST", "JAC", "BLI", "BPT", "GGG", "GGG", "SJT", "BRO", "CWA", "LCH", "PIB", "LFT", "GTR", "MLU", "LAW", "ACT", "TYR", "ABI", "GGG", "TXK", "BFM", "DHN", "FWB", "MEI", "PIB", "MCN", "ABY", "VLD", "CEC", "ACV", "MOD", "SBP", "MRY", "SMX", "RDD", "IPL", "SCK", "STT", "STX", "SBH", "CUN", "SXM", "BDA", "NAS", "MBJ", "FPO", "PUJ", "HOG", "CMW", "MEX", "ACA", "GDL", "MTY", "CZM", "VER", "HUX", "ZIH", "SJD", "SAL", "BZE", "PTY", "LIR", "SJO", "UIO", "GYE", "UIO", "GRU", "GIG", "CGH", "SDU", "EZE", "AEP", "LIM", "CUZ", "BOG", "MDE", "CTG", "GYE", "UIO", "LIM", "SCL", "EZE", "AEP", "BOG", "MDE", "CTG", "PTY", "PTY", "NAS", "MBJ", "BDA", "UVF", "PUJ", "POP", "SDQ", "SAL", "LIR", "SJO", "SAP", "RTB", "BZE", "TGU", "MGA", "BON", "CUR", "AUA", "SXM", "SAB", "EIS", "NGD", "LYB", "BDA", "VQS", "STT", "STX", "SJU", "SPB", "LRM", "AZS", "BQN", "PLS", "GCM", "BGI", "ANU", "GND", "SVD", "FDF", "DOM", "UVF", "SLU", "TAB", "POS", "BGI", "AUA", "CUR", "SDQ", "STI", "POP", "PUJ", "LRM", "PUJ", "PSE", "SIG", "MAZ", "RAI", "PAP", "JFK", "LAX", "LGA", "SFO", "MIA", "ORD", "DEN", "LAS", "DFW", "ATL", "PHX", "SEA", "CLT", "MCO", "EWR", "MSP", "DTW", "BOS", "PHL", "LGA", "FLL", "BWI", "SLC", "DCA", "IAD", "MDW", "SAN", "PDX", "TPA", "HNL", "RSW", "MCI", "AUS", "BNA", "STL", "DAL", "MSY", "HOU", "OAK", "SJC", "BHM", "PIT", "SNA", "ONT", "RDU", "SMF", "CLE", "SDF", "IND", "CMH", "MKE", "BUF", "ABQ", "MHT", "JAX", "OMA", "ORF", "SAT", "RIC", "TUL", "GSO", "HSV", "OKC", "BOI", "BUR", "TYS", "ALB", "GSP"]

departure_airport = random.choice(airport_codes)
arrival_airport = random.choice(airport_codes)




# Definiciones de Column Families
column_families_flights = [
    {
        "name": "details",
        "max_versions": 3,
        "compression": "NONE",
        "in_memory": False,
        "bloom_filter": "ROW",
        "ttl": 604800,
        "blocksize": 65536,
        "blockcache": True
    },
    {
        "name": "status",
        "max_versions": 3,
        "compression": "NONE",
        "in_memory": False,
        "bloom_filter": "ROW",
        "ttl": 259200,
        "blocksize": 32768,
        "blockcache": False
    }
]

column_families_passengers = [
    {
        "name": "personal_info",
        "max_versions": 3,
        "compression": "NONE",
        "in_memory": False,
        "bloom_filter": "ROW",
        "ttl": 604800,
        "blocksize": 65536,
        "blockcache": True
    },
    {
        "name": "travel_history",
        "max_versions": 3,
        "compression": "NONE",
        "in_memory": False,
        "bloom_filter": "ROW",
        "ttl": 259200,
        "blocksize": 32768,
        "blockcache": False
    }
]

column_families_bookings = [
    {
        "name": "details",
        "max_versions": 3,
        "compression": "NONE",
        "in_memory": False,
        "bloom_filter": "ROW",
        "ttl": 604800,
        "blocksize": 65536,
        "blockcache": True
    },
    {
        "name": "payment",
        "max_versions": 3,
        "compression": "NONE",
        "in_memory": False,
        "bloom_filter": "ROW",
        "ttl": 259200,
        "blocksize": 32768,
        "blockcache": False
    }
]

column_families_airports = [
    {
        "name": "info",
        "max_versions": 3,
        "compression": "NONE",
        "in_memory": False,
        "bloom_filter": "ROW",
        "ttl": 604800,
        "blocksize": 65536,
        "blockcache": True
    }
]

column_families_aircrafts = [
    {
        "name": "details",
        "max_versions": 3,
        "compression": "NONE",
        "in_memory": False,
        "bloom_filter": "ROW",
        "ttl": 604800,
        "blocksize": 65536,
        "blockcache": True
    },
    {
        "name": "maintenance",
        "max_versions": 3,
        "compression": "NONE",
        "in_memory": False,
        "bloom_filter": "ROW",
        "ttl": 259200,
        "blocksize": 32768,
        "blockcache": False
    }
]

# Generar datos aleatorios para las tablas
def generate_flight_data():
    flights = []
    for i in range(1000):
        flight_id = f"FL{1000 + i}"
        flights.append({
            "row_key": flight_id,
            "columns": [
                {"family": "details", "column": "flight_number", "timestamps": [random_flight_number()]},
                {"family": "details", "column": "departure_airport", "timestamps": [departure_airport]},
                {"family": "details", "column": "arrival_airport", "timestamps": [arrival_airport]},
                {"family": "details", "column": "departure_time", "timestamps": [str(random_datetime(datetime.now(), datetime.now() + timedelta(days=30)))]},
                {"family": "details", "column": "arrival_time", "timestamps": [str(random_datetime(datetime.now(), datetime.now() + timedelta(days=30)))]},
                {"family": "details", "column": "duration", "timestamps": [str(random.randint(1, 12)) + "h"]},
                {"family": "status", "column": "status", "timestamps": [random.choice(["scheduled", "in flight", "delayed", "cancelled"])]},
                {"family": "status", "column": "gate", "timestamps": [fake.bothify(text='??#')]}
            ]
        })
    return flights

def generate_passenger_data():
    passengers = []
    for i in range(1000):
        passenger_id = f"PAX{i + 1000}"
        passengers.append({
            "row_key": passenger_id,
            "columns": [
                {"family": "personal_info", "column": "first_name", "timestamps": [fake.first_name()]},
                {"family": "personal_info", "column": "last_name", "timestamps": [fake.last_name()]},
                {"family": "personal_info", "column": "email", "timestamps": [fake.email()]},
                {"family": "personal_info", "column": "phone", "timestamps": [fake.phone_number()]},
                {"family": "travel_history", "column": "flights", "timestamps": [f"FL{random.randint(1000, 1999)}"]},
                {"family": "travel_history", "column": "frequent_flyer_status", "timestamps": [random.choice(["Silver", "Gold", "Platinum"])]}
            ]
        })
    return passengers

def generate_booking_data():
    bookings = []
    for i in range(1000):
        booking_id = f"BKG{i + 1000}"
        bookings.append({
            "row_key": booking_id,
            "columns": [
                {"family": "details", "column": "flight_id", "timestamps": [f"FL{random.randint(1000, 1999)}"]},
                {"family": "details", "column": "passenger_id", "timestamps": [f"PAX{random.randint(1000, 1999)}"]},
                {"family": "details", "column": "booking_date", "timestamps": [str(fake.date_this_year())]},
                {"family": "details", "column": "seat_number", "timestamps": [f"{random.randint(1, 30)}{random.choice(['A', 'B', 'C', 'D', 'E', 'F'])}"]},
                {"family": "payment", "column": "payment_method", "timestamps": [random.choice(["Credit Card", "Debit Card", "Paypal"])]},
                {"family": "payment", "column": "amount", "timestamps": [round(random.uniform(100, 1000), 2)]},
                {"family": "payment", "column": "currency", "timestamps": ["USD"]}
            ]
        })
    return bookings

def generate_airport_data():
    airports = []
    for i in range(1000):
        airport_code = random.choice(airport_codes)
        airports.append({
            "row_key": airport_code,
            "columns": [
                {"family": "info", "column": "name", "timestamps": [fake.company()]},
                {"family": "info", "column": "city", "timestamps": [fake.city()]},
                {"family": "info", "column": "country", "timestamps": [fake.country()]},
                {"family": "info", "column": "timezone", "timestamps": [fake.timezone()]}
            ]
        })
    return airports

def generate_aircraft_data():
    aircrafts = []
    for i in range(1000):
        aircraft_id = f"AC{i + 1000}"
        aircrafts.append({
            "row_key": aircraft_id,
            "columns": [
                {"family": "details", "column": "model", "timestamps": [random.choice(["Boeing 737", "Airbus A320", "Boeing 777"])]},
                {"family": "details", "column": "manufacturer", "timestamps": [random.choice(["Boeing", "Airbus"])]},
                {"family": "details", "column": "capacity", "timestamps": [random.randint(100, 300)]},
                {"family": "details", "column": "range", "timestamps": [f"{random.randint(1000, 5000)} miles"]},
                {"family": "maintenance", "column": "last_maintenance_date", "timestamps": [str(fake.date_this_year())]},
                {"family": "maintenance", "column": "next_maintenance_date", "timestamps": [str(fake.date_this_year())]}
            ]
        })
    return aircrafts

# Generar y guardar los datos en archivos JSON

folder_path = "./datos"
os.makedirs(folder_path, exist_ok=True)

def save_to_json(table_name, data, column_families):
    file_path = os.path.join(folder_path, f"{table_name}.json")
    with open(file_path, "w") as f:
        json.dump({
            "table_name": table_name,
            "column_families": column_families,
            "data": data,
            "disabled": False
        }, f, indent=4)

# Generar datos para cada tabla
flights_data = generate_flight_data()
passengers_data = generate_passenger_data()
bookings_data = generate_booking_data()
airports_data = generate_airport_data()
aircrafts_data = generate_aircraft_data()

# Guardar datos en archivos JSON
save_to_json("flights", flights_data, column_families_flights)
save_to_json("passengers", passengers_data, column_families_passengers)
save_to_json("bookings", bookings_data, column_families_bookings)
save_to_json("airports", airports_data, column_families_airports)
save_to_json("aircrafts", aircrafts_data, column_families_aircrafts)

print("Datos generados y guardados en archivos JSON.")
