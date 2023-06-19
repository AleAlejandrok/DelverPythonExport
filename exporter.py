import csv
import sqlite3
import requests
import time
from configparser import ConfigParser

# Load the properties file
config = ConfigParser()
config.read('config.properties')

# Get the file names from the properties file
collection_db_file = config.get('Files', 'collection_db')
dl_db_file = config.get('Files', 'dl_db')
output_file = config.get('Files', 'output_file')

# Connect to the SQLite databases
collection_conn = sqlite3.connect(collection_db_file)
dl_conn = sqlite3.connect(dl_db_file)

# Create a cursor object for each connection
collection_cursor = collection_conn.cursor()
dl_cursor = dl_conn.cursor()

# Retrieve the card IDs and quantities from the SQLite databases
collection_cursor.execute("SELECT card, quantity FROM cards")
collection_data = collection_cursor.fetchall()

dl_cursor.execute("SELECT _id, scryfall_id FROM cards")
dl_data = dl_cursor.fetchall()

card_def_dict = {card_id: scryfall_id for card_id, scryfall_id in dl_data}

# Function to query Scryfall API and retrieve card details
def query_scryfall_api(scryfall_id):
    url = f'https://api.scryfall.com/cards/{scryfall_id}'
    response = requests.get(url)
    if response.status_code == 200:
        card_data = response.json()
        card_name = card_data.get('name')
        set_code = card_data.get('set')
        collector_number = card_data.get('collector_number')
        return card_name, set_code, collector_number
    print("scryfall error")
    return None, None, None

# Prepare the CSV file
csv_fields = ['Count', 'Tradelist Count', 'Name', 'Edition', 'Condition', 'Language', 'Foil', 'Tags', 'Last Modified', 'Collector Number', 'Alter', 'Proxy', 'Purchase Price']
output_csv = csv.DictWriter(open(output_file, 'w', newline='', encoding='utf-8'), fieldnames=csv_fields)
output_csv.writeheader()

# Match Scryfall IDs with quantities, query Scryfall API for card details, and write to the CSV file
start_time = time.time()
for row in collection_data:
    card_id, quantity = row
    scryfall_id = card_def_dict.get(card_id)

    if quantity is not None:
        card_name, set_code, collector_number = query_scryfall_api(scryfall_id)

        if card_name is not None and set_code is not None:
            csv_row = {
                'Count': quantity,
                'Tradelist Count': quantity,
                'Name': card_name,
                'Edition': set_code,
                'Condition': 'Near Mint',
                'Language': 'English',
                'Foil': '',
                'Tags': '',
                'Last Modified': '',
                'Collector Number': collector_number,
                'Alter': '',
                'Proxy': '',
                'Purchase Price': ''
            }
            print("matched"+card_name)
            output_csv.writerow(csv_row)

end_time = time.time()
execution_time = end_time - start_time
print(f"Execution Time: {execution_time} seconds")

# Close the database connections
collection_conn.close()
dl_conn.close()
