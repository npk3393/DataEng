
# importing the necessary python modules
import configparser
import psycopg2

# this is the python script that creates the staging, dimension tables and loads data
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    this function loads the staging tables, namely staging_events and staging_songs
    
    INPUTS:
        Cur - cursor of the connection
        Conn - database credentials with host name
    OUTPUT:
        loads the staging tables and commits the changes
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    this function inserts data into the created staging tables and dimensional tables
    
    INPUTS:
        Cur - cursor of the connection
        Conn - database credentials with host name
    OUTPUT:
        inserts the data into staging tables and dimensional tables and also, commits the changes
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    main function, reads the data warehouse configuration file, calls the loading tables and inserting tables function
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
