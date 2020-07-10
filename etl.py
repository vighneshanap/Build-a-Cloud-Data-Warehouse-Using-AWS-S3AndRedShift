import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    get all files matching to .json extension from directory and copy them to staging tables
    
    :param cur: allows interaction with the database
    :param conn: creates a new database session and returns a new connection instance
    """
    
    for query in copy_table_queries:
        print(f"Loading: {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    extract the data from staging tables and load to data warehouse tables   
    
    :param cur: allows interaction with the database
    :param conn: creates a new database session and returns a new connection instance
    """
    
    for query in insert_table_queries:
        print(f"Inserting -- {query}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()