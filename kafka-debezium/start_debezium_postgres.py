import json
import requests
import os
from dotenv import load_dotenv
from pprint import pprint
import time

# 1. Load the .env file
load_dotenv()

# -----------------------------
# CONFIG
# -----------------------------
KAFKA_CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "banking-postgres-connector"

# 2. Extract values using the correct KEYS from your .env file
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "dbname": os.getenv("POSTGRES_DB")
}

# -----------------------------
# BUILD CONNECTOR JSON
# -----------------------------
def build_connector_config():
    """Constructs the JSON payload for Debezium."""
    return {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",

            # Database connection (using variables from .env)
            "database.hostname": DB_CONFIG["host"],
            "database.port": DB_CONFIG["port"],
            "database.user": DB_CONFIG["user"],
            "database.password": DB_CONFIG["password"],
            "database.dbname": DB_CONFIG["dbname"],
            "database.server.name": "banking_server",

            # Logical decoding settings
            "plugin.name": "pgoutput",
            "slot.name": "banking_slot",
            "publication.name": "banking_publication",

            # Specific tables to watch
            "table.include.list": "public.branches,public.customers,public.employees,public.accounts,public.transactions,public.loans",
            "topic.prefix": "banking",
            "snapshot.mode": "initial",

            # Converters (JSON format without schemas for simplicity)
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",

            # Data type handling
            "decimal.handling.mode": "double",
            "time.precision.mode": "connect"
        }
    }

# -----------------------------
# API ACTIONS
# -----------------------------

def delete_connector():
    """Deletes the connector if it already exists (useful for debugging)."""
    url = f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}"
    response = requests.delete(url)
    if response.status_code == 204:
        print(f"🗑️ Existing connector '{CONNECTOR_NAME}' deleted.")

def register_connector():
    """Sends the POST request to create the connector."""
    url = f"{KAFKA_CONNECT_URL}/connectors"
    payload = build_connector_config()

    print("\n🚀 Registering Debezium Connector...")
    response = requests.post(url, json=payload)

    if response.status_code == 201:
        print("✅ Success: Connector created.")
    elif response.status_code == 409:
        print("⚠️ Warning: Connector already exists. Use delete_connector first if you want to update it.")
    else:
        print(f"❌ Failed: {response.status_code}")
        pprint(response.json())

def check_status():
    """Checks the health of the connector and its tasks."""
    url = f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/status"
    print("\n🔍 Checking Health Status...")
    response = requests.get(url)
    if response.status_code == 200:
        pprint(response.json())
    else:
        print("❌ Could not find connector status. Is it running?")

# -----------------------------
# MAIN EXECUTION
# -----------------------------
if __name__ == "__main__":
    # Uncomment delete_connector() if you are making changes and need to reset
    delete_connector() 
    register_connector()
    time.sleep(8)
    check_status()
