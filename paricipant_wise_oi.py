# link = https://nsearchives.nseindia.com/content/nsccl/fao_participant_vol_13112024.csv

import io
import psycopg2
import pandas as pd
import requests
from datetime import date, timedelta


# Function to connect to PostgreSQL
def connect_to_postgres(dbname, user, password, host='localhost'):
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host)


# Function to insert data into PostgreSQL
def insert_data_to_postgres(conn, df, table_name):
    cur = conn.cursor()

    # Clean up column names to ensure they match the database column names exactly
    columns = [col.replace(" ", "_").replace("\t", "") for col in df.columns]

    # Define the SQL query with cleaned-up column names (without quotes)
    query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
    """

    # Loop through the rows and insert them
    for _, row in df.iterrows():
        values = row.tolist()
        try:
            # Execute the insert query
            cur.execute(query, values)
            print(f"Inserted row: {row}")
        except Exception as e:
            print(f"Error inserting row: {e}")
            print(f"Row data: {row}")

    conn.commit()
    cur.close()


# Main function for scraping and storing data
def scrape_and_store_data(dbname, user, password, url_template, start_date, end_date, table_name, host='localhost'):
    conn = connect_to_postgres(dbname, user, password, host)

    headers = {
        'User-Agent': 'Mozilla/5.0'
    }

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%d%m%Y")
        url = url_template.replace("DATE_PLACEHOLDER", date_str)

        try:
            # Fetch the data from the URL
            print(f"Fetching data for date: {current_date} from URL: {url}")
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                # Load CSV data, skip the metadata row, and clean columns
                df = pd.read_csv(io.StringIO(response.text), skiprows=1)

                # Clean up the column names (remove tabs, spaces, etc.)
                df.columns = [col.strip().replace(" ", "_").replace("\t", "") for col in df.columns]

                # Ensure that the columns match exactly with the PostgreSQL table
                column_mapping = {
                    'Client_Type': 'Client_Type',
                    'Future_Index_Long': 'Future_Index_Long',
                    'Future_Index_Short': 'Future_Index_Short',
                    'Future_Stock_Long': 'Future_Stock_Long',
                    'Future_Stock_Short': 'Future_Stock_Short',
                    'Option_Index_Call_Long': 'Option_Index_Call_Long',
                    'Option_Index_Put_Long': 'Option_Index_Put_Long',
                    'Option_Index_Call_Short': 'Option_Index_Call_Short',
                    'Option_Index_Put_Short': 'Option_Index_Put_Short',
                    'Option_Stock_Call_Long': 'Option_Stock_Call_Long',
                    'Option_Stock_Put_Long': 'Option_Stock_Put_Long',
                    'Option_Stock_Call_Short': 'Option_Stock_Call_Short',
                    'Option_Stock_Put_Short': 'Option_Stock_Put_Short',
                    'Total_Long_Contracts': 'Total_Long_Contracts',
                    'Total_Short_Contracts': 'Total_Short_Contracts',
                    'Date': 'Date'
                }
                df = df.rename(columns=column_mapping)

                # Add the current date column
                df['Date'] = current_date.strftime("%Y-%m-%d")

                # Insert data into PostgreSQL
                print(f"Inserting data into table `{table_name}` with columns: {list(df.columns)}")
                insert_data_to_postgres(conn, df, table_name)

                print(f"Data inserted for {date_str}")

            else:
                print(f"Failed to fetch data for {date_str}. Status code: {response.status_code}")

        except Exception as e:
            print(f"Error processing {date_str}: {str(e)}")

        current_date += timedelta(days=1)

    conn.close()
    print("Database connection closed.")


# Example of running the script
if __name__ == '__main__':
    dbname = "indices"
    user = 'postgres'
    password = 'mosl@123'
    host = 'localhost'
    url_template = "https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_DATE_PLACEHOLDER.csv"

    start_date = date(2024, 10, 13)  
    end_date = date(2024, 10, 30)
    table_name = "pwoi"

    print("Connected to PostgreSQL database.")
    scrape_and_store_data(dbname, user, password, url_template, start_date, end_date, table_name, host)
    print("Process finished.")
