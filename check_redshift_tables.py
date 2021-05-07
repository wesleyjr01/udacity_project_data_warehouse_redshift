import configparser
import psycopg2


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    query = "SELECT * FROM PG_TABLE_DEF WHERE schemaname = 'public';"
    cur.execute(query)
    records = cur.fetchall()

    for record in records:
        print(record)

    conn.close()


if __name__ == "__main__":
    main()
