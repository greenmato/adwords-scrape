import mysql.connector
import logging
import json
import adwords_scrape.spiders.database.db_config as config


class DbConnection:
    def __init__(self):
        self.connection = mysql.connector.connect(
            user=config.mysql['user'],
            password=config.mysql['password'],
            host=config.mysql['host'],
            database=config.mysql['database']
        )

    def __del__(self):
        self.connection.close()

    def import_keywords(self):
        cursor = self.connection.cursor(buffered=True)

        cursor.execute("SELECT id, keyword FROM keywords")

        results = [
            {
                'id': row[0],
                'keyword': row[1].decode("utf-8")
            } for row in cursor.fetchall()
        ]

        cursor.close()

        return results

    def insert_result(self, result):
        cursor = self.connection.cursor(buffered=True)

        columns = tuple(column for column, value in result.items())
        values = tuple(json.dumps(value) if type(value) is list else value for column, value in result.items())

        placeholder = ','.join(['%s' for value in values])

        sql = ("INSERT INTO scrape_results (%s)" % placeholder)
        sql = (sql % columns)
        sql = (sql + " VALUES (%s)" % placeholder)

        logging.info(sql)
        logging.info(values)

        cursor.execute(sql, values)

        self.connection.commit()

        cursor.close()
