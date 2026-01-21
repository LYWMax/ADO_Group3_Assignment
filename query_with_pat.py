import os
import snowflake.connector

def main():
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USERNAME'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        role=os.environ['SNOWFLAKE_ROLE'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA']
    )

    cursor = conn.cursor()
    try:
        cursor.execute("SELECT CURRENT_TIMESTAMP(), CURRENT_USER(), CURRENT_ROLE();")
        for row in cursor:
            print("Query Result:", row)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
