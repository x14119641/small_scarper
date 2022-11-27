from db import ConnectorDb as cdb
from scarper import Scarper as sc
from datetime import datetime
import pandas as pd
import os

errors_dicts_list = []

def main():
    cls = sc(cdb())


def set_up():
    conn = cdb()
    conn.create_schema()


def insert_ticks():
    conn = cdb()
    cls = sc()
    bulk_data = []
    i=0
    for index, row in cls.tickers.iterrows():
        i += 1
        bulk_data.append((row['Symbol'], row["Name"]))
        if i % 999 ==0:
            conn.execute_query(
                "INSERT INTO tickers(tick, name) VALUES (?,?)",bulk_data)
            bulk_data = []
    conn.execute_query(
                "INSERT INTO tickers(tick, name) VALUES (?,?)",bulk_data)
    print('data inserted in tickers')

def insert_metadata():
    conn = cdb()
    cls = sc()
    bulk_data = []
    i=0
    cls.tickers.fillna('', inplace=True)
    for index, row in cls.tickers.iterrows():
        i += 1
        bulk_data.append((
            row['Symbol'], row["Name"], 
            row["Last Sale"].replace('$',''), 
            row["Net Change"],
            row["% Change"].replace('%',''),
            row["Market Cap"],row["Country"],
            row["IPO Year"], row["Volume"],
            row["Sector"],row["Industry"]
            ))
        if i % 999 ==0:
            print(bulk_data)
            conn.execute_query(
                """INSERT INTO metadata(
                    tick, name, last_sale, net_change, change_perc,
                    market_cap, country, ipo_year, volume, sector, industry) 
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    bulk_data)
            bulk_data = []
    conn.execute_query(
        """INSERT INTO metadata(
            tick, name, last_sale, net_change, change_perc,
            market_cap, country, ipo_year, volume, sector, industry) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            bulk_data)
    print('data inserted in metadata')


def insert_investors_data(refresh_date=None):
    conn = cdb()
    cls = sc()
    bulk_data = []
    i=0
    cls.tickers.fillna('', inplace=True)
    for tick in cls.tickers['Symbol'][:]:
        i += 1
        try:    
            data = cls.parse_data(cls.get_data(tick))
            bulk_data.append((
                tick, data['SharesOutstandingPCT'],
                data['ShareoutstandingTotal'],data['TotalHoldingsValue'],
                data['IncreasedPositionsHolders'],data['IncreasedPositionsShares'],
                data['DecreasedPositionsHolders'],data['DecreasedPositionsShares'],
                data['HeldPositionsHolders'],data['HeldPositionsShares'],
                data['TotalInstitutionalHolders'],data['TotalInstitutionalShares'],
                data['NewPositionsHolders'],data['NewPositionsShares'],
                data['SoldOutPositionsHolders'],data['SoldOutPositionsShares'],
                refresh_date or ''
                ))
            if i % 99 ==0:
                print(i+1)
            if i % 999 ==0:
                    conn.execute_query(
                        """INSERT INTO institutional_holdings (
                            tick, institutional_ownership_perc, 
                            total_shares_outstanding_millions, total_value_holdings_millions, 
                            increased_positions_holders,increased_positions_shares,
                            decreased_positions_holders, decreased_positions_shares,
                            held_positions_holders,held_positions_shares,
                            total_institutional_holders,total_institutional_shares,
                            new_positions_holders,new_positions_shares,
                            sold_out_positions_holders,sold_out_positions_shares,
                            refreshed_page_date) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            bulk_data)
                    bulk_data = []
            if i ==len(cls.tickers['Symbol'])-1:
                conn.execute_query(
                    """INSERT INTO institutional_holdings(
                        tick, institutional_ownership_perc, 
                        total_shares_outstanding_millions, total_value_holdings_millions, 
                        increased_positions_holders,increased_positions_shares,
                        decreased_positions_holders, decreased_positions_shares,
                        held_positions_holders,held_positions_shares,
                        total_institutional_holders,total_institutional_shares,
                        new_positions_holders,new_positions_shares,
                        sold_out_positions_holders,sold_out_positions_shares,
                        refreshed_page_date) 
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        bulk_data)
            
        except Exception as e:
            # print('e: ',str(e))
            
            errors_dicts_list.append({
                'tick':tick, 'position':i, 'error':str(e)
            })
    print('data inserted in instutitions')
    pd.DataFrame(
        errors_dicts_list).to_csv(
            'errors_runner.csv', mode='a', header=not os.path.exists('errors_runner.csv'))

    

if __name__=="__main__":
    refresh_date = datetime.strptime(
        'Wed, 09 Nov 2022 17:49:42', 
        '%a, %d %b %Y %H:%M:%S').strftime('%d-%m-%Y %H:%M:%S')
    # set_up()
    # insert_ticks()
    # insert_metadata()
    insert_investors_data(refresh_date)
    # datetime.strptime('Wed, 09 Nov 2022 17:49:42', '%a, %d %b %Y %H:%M:%S') 