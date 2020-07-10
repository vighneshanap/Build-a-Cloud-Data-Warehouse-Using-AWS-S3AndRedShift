import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

 
def drop_tables(cur, conn):
    """
     check the existing tables, drop if exists
    
    :param cur: allows interaction with the database
    :param conn: creates a new database session and returns a new connection instance
    """
    
    for query in drop_table_queries:
        print(f"Droping: {query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    create tables from the sql queries
    
    :param cur: allows interaction with the database
    :param conn: creates a new database session and returns a new connection instance
    """
    
    for query in create_table_queries:
        print(f"Creating: {query}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
