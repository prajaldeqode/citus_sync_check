import psycopg2
import schedule
import time
import os
from dotenv import load_dotenv
load_dotenv()
from utils.tables import tables
from utils.send_metrics import expose_difference_in_vid
# Database connection details
citus_config = {
    'host': os.environ.get("CITUS_HOST"),
    'database': os.environ.get("CITUS_DATABASE"),
    'user': os.environ.get("CITUS_USERNAME"),
    'password': os.environ.get("CITUS_PASSWORD"),
    'port':int(os.environ.get("CITUS_PORT"))
}

graphnode_arbitrum_config = {
    'host': os.environ.get("GRAPH_NODE_ARBITRUM_HOST"),
    'database': os.environ.get("GRAPH_NODE_ARBITRUM_DATABASE"),
    'user': os.environ.get("GRAPH_NODE_ARBITRUM_USERNAME"),
    'password': os.environ.get("GRAPH_NODE_ARBITRUM_HOST"),
    'port': int(os.environ.get("GRAPH_NODE_ARBITRUM_PORT"))
}

def createConnection(db_config):
    connection = psycopg2.connect(**db_config)
    return connection

citus_connection = createConnection(citus_config)
graphnode_connection = createConnection(graphnode_arbitrum_config)

def fetch_data(fetchFor, db_config, table, column):
    global citus_connection
    global graphnode_connection
    connection = citus_connection if fetchFor == "citus" else graphnode_connection
    for itr in range(3):
        try:
            if(connection.closed):
                raise Exception(f"Connection is closed for {fetchFor}")
            query = f"SELECT {column} FROM {table} order by {column} desc limit 1"
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return result[0][0]
        except Exception as e:
            if(itr == 2 or str(e) != f"Connection is closed for {fetchFor}"):
                raise e
            print(e)
            print(f"connection reset for {fetchFor}")
            if(fetchFor == "citus"):
                citus_connection = createConnection(db_config=db_config)
                connection = citus_connection
            else:
                graphnode_connection = createConnection(db_config=graphnode_arbitrum_config)
                connection = graphnode_connection

def calculate_difference(vid1, vid2):
    return abs(vid1 - vid2)
    
def job():
    for table in tables:
        try:
            citus_data = fetch_data(fetchFor = "citus", db_config=citus_config, table=table["table"], column=table["column"])
            graphnode_arbitrum_data = fetch_data(fetchFor = "graphnod_arbitrum", db_config=graphnode_arbitrum_config, table=table["table"], column=table["column"])
            print(table["table"],citus_data,graphnode_arbitrum_data)
            if(type(citus_data) != int):
                raise Exception(f"Data is not fetched from {table['table']} table in citus database")
            if(type(graphnode_arbitrum_data) != int):
                raise Exception(f"Data is not fetched from {table['table']} table in graph_node_arbitrum database")
            expose_difference_in_vid(
                table_name=table["table"] ,
                difference=calculate_difference(vid1=citus_data,vid2=graphnode_arbitrum_data)
            )
        except Exception as e:
            print(e)

# Schedule the job every 5 minutes
schedule.every(2).minutes.do(job)

def main():
    job()  # Run the job immediately on startup
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
