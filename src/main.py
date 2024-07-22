import psycopg2
import schedule
import time
import os
from dotenv import load_dotenv
load_dotenv()
from utils.tables import tables
from utils.send_metrics import expose_difference_in_vid
from utils.send_logs_to_loki import send_log_to_loki

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

def fetch_vids(db_config, table, column):
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()
    query = f"SELECT {column} FROM {table} order by {column} desc limit 1"
    cursor.execute(query)
    result = cursor.fetchall()
    connection.close()
    return [row[0] for row in result]

def calculate_difference(vids1, vids2):
    return abs(sum(vids1) - sum(vids2))
    
def job():
    for table in tables:
        try:
            citus_vids = fetch_vids(db_config=citus_config, table=table["table"], column=table["column"])
            graphnode_arbitrum_vids = fetch_vids(db_config=graphnode_arbitrum_config, table=table["table"], column=table["column"])
            if(type(citus_vids) != list):
                raise Exception(f"Data is not fetched from {table['table']} table in citus database")
            if(type(graphnode_arbitrum_vids) != list):
                raise Exception(f"Data is not fetched from {table['table']} table in graph_node_arbitrum database")
            expose_difference_in_vid(
                table_name=table["table"] ,
                difference=calculate_difference(vids1=citus_vids,vids2=graphnode_arbitrum_vids)
            )
        except Exception as e:
            send_log_to_loki(log_message=e,labels={"job":"symmetric-ds","table_name":table})

# Schedule the job every 5 minutes
schedule.every(1).minutes.do(job)

def main():
    job()  # Run the job immediately on startup
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
