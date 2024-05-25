import sqlite3
import json


def schema():
    return """
        CREATE TABLE list_table (
        list_id TEXT,
        name INTEGER,
        year_state TEXT PRIMARY KEY,
        price_all REAL,
        price_com REAL,
        price_ind REAL,
        price_res REAL
    );
        """


def run():
    data = []
    with open("delhi_final_cleaned.json", "r") as f:
        data = json.load(f)

    conn = sqlite3.connect("twitter_user_data.db")
    cursor = conn.cursor()

    first_dict = data[0]
    keys = list(first_dict.keys())

    # Generate the CREATE TABLE query dynamically
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS twitter_users (
            {', '.join(f'{key} TEXT' for key in keys)}
        )
    """

    # Create the table
    cursor.execute(create_table_query)

    # Convert each dictionary to JSON and insert into the table
    for entry in data:
        # Generate the INSERT INTO query dynamically
        entry["token"] = " ".join(entry["token"])
        insert_query = f"""
            INSERT INTO twitter_users VALUES ({', '.join('?' for _ in range(len(keys)))})
        """

        # Insert data into the table
        cursor.execute(insert_query, tuple(entry[key] for key in keys))

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
