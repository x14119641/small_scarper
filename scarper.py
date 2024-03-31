import requests
import pandas as pd
from db import ConnectorDb as cdb




class Scarper():
    errors_dicts_list = []

    def __init__(self, tickers=True) -> None:
        if tickers:
            self.tickers = pd.read_csv('tickers2.csv')


    def get_data(self, tick):
        URL = f"https://api.nasdaq.com/api/company/{tick}/institutional-holdings?limit=15&type=TOTAL&sortColumn=marketValue&sortOrder=DESC"
        HEADERS = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Mobile Safari/537.36",
            "Content-Type":"application/json"}

        data = requests.get(URL,headers=HEADERS).json()
        return data

    def parse_data(self, data):
        return {
            "SharesOutstandingPCT":data['data']['ownershipSummary']['SharesOutstandingPCT']['value'].replace('%', ''),
            "ShareoutstandingTotal":data['data']['ownershipSummary']['ShareoutstandingTotal']['value'],
            "TotalHoldingsValue":data['data']['ownershipSummary']['TotalHoldingsValue']['value'].replace('$', ''),
            "IncreasedPositionsHolders":data['data']['activePositions']['rows'][0]['holders'],
            "IncreasedPositionsShares":data['data']['activePositions']['rows'][0]['shares'].replace(',', ''),
            "DecreasedPositionsHolders":data['data']['activePositions']['rows'][1]['holders'],
            "DecreasedPositionsShares":data['data']['activePositions']['rows'][1]['shares'].replace(',', ''),
            "HeldPositionsHolders":data['data']['activePositions']['rows'][2]['holders'],
            "HeldPositionsShares":data['data']['activePositions']['rows'][2]['shares'].replace(',', ''),
            "TotalInstitutionalHolders":data['data']['activePositions']['rows'][3]['holders'],
            "TotalInstitutionalShares":data['data']['activePositions']['rows'][3]['shares'].replace(',', ''),
            "NewPositionsHolders":data['data']['newSoldOutPositions']['rows'][0]['holders'],
            "NewPositionsShares":data['data']['newSoldOutPositions']['rows'][0]['shares'].replace(',', ''),
            "SoldOutPositionsHolders":data['data']['newSoldOutPositions']['rows'][1]['holders'],
            "SoldOutPositionsShares":data['data']['newSoldOutPositions']['rows'][1]['shares'].replace(',', ''),
        }


    def main(self):
        print(len(self.tickers))
        for i in range(1,len(self.tickers[0:2])):
            print(i)
            try:    
                print('tick: ', self.tickers.iloc[i].Symbol)
                data = self.parse_data(self.get_data(self.tickers.iloc[i].Symbol))
                print('data:', data)
            except Exception as e:
                
                self.errors_dicts_list.append({
                    'tick':self.tickers.iloc[i], 'position':i, 'error':str(e)
                })
        print('finito')
        
    

    # def __del__(self):
    #     pd.DataFrame(self.errors_dicts_list).to_csv('errors.csv')


if __name__ == '__main__':
    cls = cdb()
    sc = Scarper(cls)
    cls.show_dbs()
    sc.main()
