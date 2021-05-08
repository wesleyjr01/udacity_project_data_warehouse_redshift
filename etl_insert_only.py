import configparser
import psycopg2
from sql_queries import artist_table_insert, artist_table_drop, artist_table_create


def insert_tables(cur, conn):
    query = artist_table_insert
    cur.execute(query)
    conn.commit()


def drop_artist(cur, conn):
    query = artist_table_drop
    cur.execute(query)
    conn.commit()


def create_artist(cur, conn):
    query = artist_table_create
    cur.execute(query)
    conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={}\
         port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    # load_staging_tables(cur, conn)
    print("dropping artists table...")
    drop_artist(cur, conn)
    print("creating artists table...")
    create_artist(cur, conn)
    print("inserting artists table...")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
