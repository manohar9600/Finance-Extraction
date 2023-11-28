import psycopg2
import pandas as pd


class DBFunctions:
    def __init__(self) -> None:
        pass

    @staticmethod
    def _get_connection():
        conn = psycopg2.connect(
            database="ProdDB", host="localhost", user="postgres", password="manu1234", port="5432"
        )
        return conn

    def get_table_data(self, table_name: str,  filter_dict={}) -> pd.DataFrame:
        conn = self._get_connection()
        cursor = conn.cursor()
        query = f'select * from {table_name}'
        if filter_dict:
            query += ' where'
            for key in filter_dict:
                query += f' {key}={filter_dict[key]}'
        cursor.execute(query)
        df = pd.DataFrame(cursor, columns=[desc[0] for desc in cursor.description])
        conn.close()
        return df
    