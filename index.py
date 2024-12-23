import mysql.connector
import pandas as pd

# Step 1: Connect to MySQL database
db_connection = mysql.connector.connect(
    host='db.docs.unicommerce.infra',
    user='root',  # replace with your username
    password='uniware',  # replace with your password
    database='unidocs'
)

START_DATE = '2024-05-13 15:32:18'
END_DATE = '2024-06-13 15:32:18'
TENANT_CODE = 'rarerabbit'

def extract_between_hyphens(input_string):
    # Find the position of the first hyphen
    first_hyphen_index = input_string.find('-')
    
    # Find the position of the last hyphen
    last_hyphen_index = input_string.rfind('-')
    
    # Extract the substring between the first and last hyphen
    if first_hyphen_index != -1 and last_hyphen_index != -1 and first_hyphen_index < last_hyphen_index:
        return input_string[first_hyphen_index + 1:last_hyphen_index]
    else:
        return None  # Return None if there are no valid hyphens


# Step 2: Load the database and select data from the 'document' table
def fetch_data_from_db():
    try:
        cursor = db_connection.cursor()
        
        
        where_clause = f" identifier like 'PO-%-{TENANT_CODE}' AND created between '{START_DATE}' and '{END_DATE}' "
        query = f"SELECT identifier, username, created, CONCAT('https://docs.unicommerce.com/files/', document_location) FROM document where {where_clause}"
        
        print(f"executing query: {query}")
        cursor.execute(query)
        data = cursor.fetchall()

        data_formatted = [ (extract_between_hyphens(col[0]), col[1], col[2], col[3]) for col in data ]
        print(data_formatted)
        # Cleanup: close cursor and connection
        cursor.close()
        db_connection.close()
        

        return data_formatted
    
    except Exception as e:
        print(f"ERROR: fetch_data_from_db: {e}")
        return None
    


try:        
    data = fetch_data_from_db()
    headers = ["PO Code", "UserName", "Date", "Document Link"]
    df = pd.DataFrame(data, columns=headers)
    df.to_csv('report.csv', index=False)

except Exception as e:
    print("Error: ", e)

