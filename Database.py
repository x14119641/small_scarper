from contextlib import contextmanager
from tools import get_logger
import sqlite3

DB = 'db.db'
TEST_DB = 'test.db'


class Database:

    def __init__(self, db_name: str = TEST_DB) -> None:
        self.db_name = db_name
        self.logger = get_logger()
        

    @contextmanager
    def connect(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_name)
            conn.row_factory = sqlite3.Row
            yield conn
            if conn is not None:
                conn.commit()
        except Exception as e:
            err_msn = f'<Error> Error in Database: {str(e)}'
            self.logger.error(err_msn)
            raise e
        finally:
            if conn is not None:
                conn.close()


    def execute(self, query: str, params: dict = None):
        with self.connect() as conn:
            cursor = conn.cursor()
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            cursor.close()

    def fetchone(self, query: str, params: dict = None):
        with self.connect() as conn:
            cursor = conn.cursor()
            if params is None:
                row = cursor.execute(query).fetchone()
            else:
                row = cursor.execute(query, params).fetchone()
            cursor.close()
        if row:
            return row
        return

    def fetchall(self, query: str, params: dict = None):
        with self.connect() as conn:
            cursor = conn.cursor()
            if params is None:
                rows = cursor.execute(query).fetchall()
            else:
                rows = cursor.execute(query, params).fetchall()
            cursor.close()
        if rows:
            return [dict(row) for row in rows]
        return


