import sqlite3

def show_users_table(db_file, table_name='users'):
    """
    Displays the content of the users table in the SQLite database.
    
    Args:
        db_file (str): Path to the SQLite database file.
        table_name (str): Name of the table to display (default: 'users').
    """
    try:
        # Connect to SQLite database
        connection = sqlite3.connect(db_file)
        cursor = connection.cursor()

        # Check if the table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if not cursor.fetchone():
            print(f"Table '{table_name}' does not exist.")
            return

        # Fetch and display table content
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()

        if rows:
            # Fetch column names
            column_names = [description[0] for description in cursor.description]

            # Print the table content
            print(f"\nContent of the '{table_name}' table:")
            print("-" * 50)
            print(f"{' | '.join(column_names)}")
            print("-" * 50)
            for row in rows:
                print(" | ".join(map(str, row)))
            print("-" * 50)
        else:
            print(f"The table '{table_name}' is empty.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if connection:
            connection.close()

# Example usage
show_users_table('dags/db.sqlite')
