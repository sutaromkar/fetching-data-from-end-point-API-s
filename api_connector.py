#https://jsonplaceholder.typicode.com/todos/
import argparse
import os

import pandas
import psycopg2
import requests

from shared_internal import logger as custom_logger
logger = custom_logger.get_logger(__name__)

# Environment variables for Database connection
db_name = os.environ['db_name']
host = os.environ['db_host']
port = os.environ['db_port']
username = os.environ['db_username']
password = os.environ['db_password']


def open_connection():
    dsn = f"dbname='{db_name}' host='{host}' port='{port}' user='{username}' password='{password}'"
    global conn
    try:
        conn = psycopg2.connect(dsn)
    except Exception as e:
        logger.error(f"Database connection error: {e}")


def check_connection():
    if 0 == conn.closed:
        open_connection()


open_connection()


# Fetch data from Endpoint API's
def fetch_data(url_name, user_id):
    try:
        headers = {'User-Agent': 'python-requests'}
        if user_id:
            url_name = url_name + '?userId=' + user_id
            #https://jsonplaceholder.typicode.com/posts?userId=1
        get_resp = requests.get(url=url_name, headers=headers)
        if get_resp.status_code == 200:
            resp_data = get_resp.json()
            logger.info(resp_data)
            store_data_to_csv(resp_data)
            store_data_to_database(resp_data)
        elif get_resp.status_code == 404:
            logger.info("Url not found")
    except Exception as e:
        logger.error(f"Exception in Get Data: {e}")


# Store data to csv file
def store_data_to_csv(data):
    try:
        df = pandas.DataFrame(data, columns=['userId', 'id', 'title', 'body'])
        df.to_csv('data.csv', index=False, sep="|")
    except Exception as e:
        logger.error(f"Exception in storing data to csv: {e}")


# Store data to Database
def store_data_to_database(data):
    try:
        check_connection()
        for single_data in data:
            # save the new item
            insert_statement = \
                """INSERT INTO posts_data (userid, id, title, body)
                    VALUES (%(p_userId)s, 
                    %(p_id)s, 
                    %(p_title)s, 
                    %(p_body)s);
                """
            cur = conn.cursor()

            query_insert = cur.mogrify(insert_statement, {"p_userId": single_data.get("userId"),
                                                          "p_id": single_data.get("id"),
                                                          "p_title": single_data.get("title"),
                                                          "p_body": single_data.get("body")})

            cur.execute(query_insert)
            cur.connection.commit()
            # query_for_single_item(cur, logger, query_insert)
    except Exception as e:
        logger.error(f"Exception in storing data to Database: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="API Connector Data")
    parser.add_argument("-u", "--url_name", help="URL Name", required=True)
    parser.add_argument("-i", "--user_id", help="User id value", required=False, default=None)

    args = parser.parse_args()
    try:
        fetch_data(**vars(args))
    except Exception as e:
        logger.error(f"Exception in fetching data: {e}")
