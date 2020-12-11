
# importing the necessary python modules
import configparser
import psycopg2
# invoking the python script that creates the tables using Redshift DDL, loads data into them
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    this function drops ALL tables -  the staging tables, and dimensional tables 
    this function uses "DROP TABLE IF EXISTS" so the script doesn’t fail if DROP TABLE runs against a nonexistent table
    
    INPUTS:
        Cur - cursor of the connection
        Conn - database credentials with host name
    OUTPUT:
        drops the staging tables and commits the changes
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    this function executes my DDL scripts and thereby creating, the staging tables, and dimensional tables 
    this function uses "IF EXISTS" so the script doesn’t fail if CREATE TABLE runs against a existent table
    
    INPUTS:
        Cur - cursor of the connection
        Conn - database credentials with host name
    OUTPUT:
        executes the DDL, creates tables and commits the changes
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    this main function reads the data warehouse configuration file and calls the drop_tables and create_tables function
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
