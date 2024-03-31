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
    FOREIGN KEY (tick) REFERENCES fk_metadata_tickers (tick)
)