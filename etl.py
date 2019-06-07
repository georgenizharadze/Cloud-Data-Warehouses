import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Copy data from S3 to Redshift staging tables

    Enables copying at scale to Redshift tables
    directly from the JSON files residing in S3. Loops
    through the copy queries defined in the 'sql_queries.py'
    file. 

    Arguments:
    cur -- PostgreSQL (Redshift) cursor object of 
    conn -- PostgreSQL (Redshift) connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from Redshift staging to final tables

    Queiries the staging tables and inserts data into the
    final star schema tables of the Redshift data warehouse.
    Also performs certain transformations, particularly on the
    timestamp data. Loops through the copy queries defined in 
    the 'sql_queries.py' file. 

    Arguments:
    cur -- PostgreSQL (Redshift) cursor object of 
    conn -- PostgreSQL (Redshift) connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg') # connection parameters reside in the config file

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".
                            format(config.get('CLUSTER', 'HOST'),
                                   config.get('CLUSTER', 'DB_NAME'),
                                   config.get('CLUSTER', 'DB_USER'),
                                   config.get('CLUSTER', 'DB_PASSWORD'),
                                   config.get('CLUSTER', 'DWH_PORT')))

    print("Connected to the cluster.")
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print("Finished loading staging tables.")
    insert_tables(cur, conn)
    print("Finished inserting into final tables.")

    conn.close()


if __name__ == "__main__":
    main()