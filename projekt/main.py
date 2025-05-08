from mysql_connector import MySQLConnector
from postgres_connector import PostgreSQLConnector


def main():
    mysql_connector = MySQLConnector(host='localhost', database='mydb', user='user', password='password')
    postgres_connector = PostgreSQLConnector(dbname='mydb', user='user', password='password', host='localhost')

    mysql_connector.drop_tables()
    postgres_connector.drop_tables()

    ### MYSQL SECTION
    mysql_connector.create_tables()
    mysql_connector.insert_fake_data()

    ### POSTGRESQL SECTION
    postgres_connector.create_tables()
    postgres_connector.insert_fake_data()



if __name__ == '__main__':
    main()
