import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop all tables in case they exist

    To build a data warehouse from scratch and avoid
    any potential errors due to existing tables.

    Arguments:
    cur -- PostgreSQL (Redshift) cursor object of 
    conn -- PostgreSQL (Redshift) connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all the required tables

    Create both the staging and final data warehouse
    tables. The function loops through the list of the
    create queries defined in the 'sql_queries.py' file.

    Arguments:
    cur -- PostgreSQL (Redshift) cursor object of 
    conn -- PostgreSQL (Redshift) connection object
    """  
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".
                            format(config.get('CLUSTER', 'HOST'),
                                   config.get('CLUSTER', 'DB_NAME'),
                                   config.get('CLUSTER', 'DB_USER'),
                                   config.get('CLUSTER', 'DB_PASSWORD'),
                                   config.get('CLUSTER', 'DWH_PORT')))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("Finished creating tables.")


if __name__ == "__main__":
    main()