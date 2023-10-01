import sqlite3


class ConnectorDb():

    def __init__(self) -> None:
        self.conn = sqlite3.connect("db.db")


    def create_schema(self):
        sql = """
            CREATE TABLE IF NOT EXISTS tickers (tick text PRIMARY KEY, name text)
        """
        self.execute_query(sql)
        sql = """
            CREATE TABLE IF NOT EXISTS metadata (
                id integer PRIMARY KEY,
                tick text NOT NULL, 
                name text,
                last_sale REAL,
                net_change REAL,
                change_perc REAL,
                market_cap INTEGER,
                country TEXT,
                ipo_year INTEGER,
                volume INTEGER,
                sector TEXT,
                industry TEXT,
                inserted TIMESTAMP DEFAULT (strftime('%d-%m-%Y %H:%M:%S', 'now')),
                FOREIGN KEY (tick)
                REFERENCES fk_metadata_tickers (tick)
            )
        """
        self.execute_query(sql)
        sql = """
            CREATE TABLE IF NOT EXISTS institutional_holdings (
                id integer PRIMARY KEY,
                tick text NOT NULL, 
                institutional_ownership_perc REAL,
                total_shares_outstanding_millions INTEGER,
                total_value_holdings_millions INTEGER,
                increased_positions_holders INTEGER,
                increased_positions_shares INTEGER,
                decreased_positions_holders INTEGER,
                decreased_positions_shares INTEGER,
                held_positions_holders INTEGER,
                held_positions_shares INTEGER,
                total_institutional_holders INTEGER,
                total_institutional_shares INTEGER,
                new_positions_holders INTEGER,
                new_positions_shares INTEGER,
                sold_out_positions_holders INTEGER,
                sold_out_positions_shares INTEGER,
                refreshed_page_date TIMESTAMP DEFAULT NULL,
                inserted TIMESTAMP DEFAULT (strftime('%d-%m-%Y %H:%M:%S', 'now')),
                FOREIGN KEY (tick)
                REFERENCES fk_institutional_holdings_tickers (tick)
            )
        """
        self.execute_query(sql)


    def test_create(self):
        with self.conn as conn: 
            conn.execute("CREATE TABLE movie(title, year, score)")
        
    
    def test_many(self):
        sql = "INSERT INTO movie VALUES(?, ?, ?)"
        data = [
            ("Monty Python Live at the Hollywood Bowl", 1982, 7.9),
            ("Monty Python's The Meaning of Life", 1983, 7.5),
            ("Monty Python's Life of Brian", 1979, 8.0),
        ]
        self.execute_query(sql,data)


    def test_main(self):
        self.test_create()
        self.test_many()
        data = self.get_query('SELECT * FROM movie')
        print(data)
        self.show_dbs()


    def show_dbs(self):
        with self.conn as conn:
            data = conn.execute("SELECT name FROM sqlite_master  WHERE type='table'").fetchall()
            print(list(data))


    def execute_query(self, sql, data=None):
        with self.conn as conn:
            if data:
                conn.executemany(sql,data)
            else:
                conn.execute(sql)
        

    def get_query(self, sql, data=None):
        with self.conn as conn:
            if data:
                data = conn.executemany(sql,data).fetchall()
            else:
                data = conn.execute(sql).fetchall()
        return data


    def delete_db(self):
        with self.conn as conn: 
            conn.execute("DROP TABLE IF EXISTS institutional_holdings")
            conn.execute("DROP TABLE IF EXISTS metadata")
            conn.execute("DROP TABLE IF EXISTS tickers")


    def __del__(self):
        with self.conn as conn:
            conn.execute("DROP TABLE IF EXISTS movie")
        
    

if __name__=="__main__":
    cls = ConnectorDb()
    print(cls.get_query("SELECT COUNT(*) FROM tickers"))
    print(cls.get_query("SELECT COUNT(*) FROM institutional_holdings"))
    print(cls.get_query("SELECT * FROM tickers WHERE tick = 'SHOT'"))
    # print(cls.get_query("SELECT * FROM institutional_holdings"))
    # cls.execute_query("DELETE FROM institutional_holdings")
    # cls.test_main()
    # cls.delete_db()
    