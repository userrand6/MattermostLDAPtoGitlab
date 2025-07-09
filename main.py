#!/usr/bin/env python3
"""
Fetch every user from Authentik and keep
(id, username) pairs in a list called user_records.
"""

import os
import subprocess
from datetime import datetime
import requests # pip3 install requests
from dotenv import load_dotenv # pip3 install python-dotenv

import os, psycopg2 # pip3 install psycopg2 (needs brew install postgresql /sudo apt-get install libpq-dev)

from psycopg2.extras import execute_batch


load_dotenv()

BASE_URL   = os.getenv("AK_URL").rstrip("/")
TOKEN      = os.getenv("AK_TOKEN")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
CREATE_DUMP_BEFORE = bool(os.getenv("CREATE_DUMP_BEFORE"))
DUMP_PATH = os.getenv("DUMP_PATH")



DATABASE   = connection_string = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"

PAGE_SIZE  = 500          # tweak if you have many users

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
})

def get_all_users():
    """Generator that yields every user object."""
    url = f"{BASE_URL}/api/v3/core/users/?page_size={PAGE_SIZE}"
    while url:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        for user in data["results"]:
            yield user
        url = data.get("next")   # Authentik returns absolute URL for next page, or None

# --- Build the list ----------------------------------------------------------
user_records = [
    {"id": u["pk"], "username": u["username"]}
    for u in get_all_users()
]

# print(f"Collected {len(user_records)} users")
# print(user_records)            # or write to JSON / CSV, etc.

if CREATE_DUMP_BEFORE:
    # Set environment variable to allow pg_dump to use the password
    os.environ["PGPASSWORD"] = DB_PASSWORD  # This is where we provide the password

    # Get the current date and time
    current_time = datetime.now()

    # Format the current time as a string (you can customize the format)
    timestamp = current_time.strftime("%Y-%m-%d_%H-%M-%S")

    # Define the dump file path
    backup_file = f"{str(DUMP_PATH)}database_dump_{timestamp}.sql"

    # # Run the pg_dump command to create a backup
    # pg_dump_command = [
    #     "pg_dump",
    #     f"--{DB_NAME}=postgresql://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    #     "--file", backup_file,
    #     "--format=c"  # Custom format (compressed)
    # ]
    pg_dump_command = [
        "pg_dump",
        "-U", DB_USER,
        "-h", DB_HOST,
        "-p", DB_PORT,
        "-d", DB_NAME,
        "-F", "c",  # Custom format (compressed)
        "--file", backup_file
    ]

    try:
        subprocess.run(pg_dump_command, check=True)
        print(f"Backup successful: {backup_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error during backup: {e}")
    finally:
        # Clean up the password environment variable for security
        del os.environ["PGPASSWORD"]  # It's important to delete it after use for security


with psycopg2.connect(DATABASE) as conn:
    with conn.cursor() as cur:
        update_sql = """
            UPDATE users
               SET authservice = %s,
                   authdata = %s
             WHERE username = %s;
        """
        # Prepare values: (authtype, authdata, username)
        params = [("gitlab", rec["id"], rec["username"]) for rec in user_records]

        # execute_batch sends many updates in one round‑trip
        execute_batch(cur, update_sql, params)

    conn.commit()

print(f"Updated {len(user_records)} rows in users table.")