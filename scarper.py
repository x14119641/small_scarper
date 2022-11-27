import requests
import pandas as pd




class Scarper():
    errors_dicts_list = []

    def __init__(self, tickers=True) -> None:
        if tickers:
            self.tickers = pd.read_csv('tickers.csv')['Symbol']


    def get_data(self, tick):
        URL = f"https://api.nasdaq.com/api/company/{tick}/institutional-holdings?limit=15&type=TOTAL&sortColumn=marketValue&sortOrder=DESC"
        HEADERS = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Mobile Safari/537.36",
            "Content-Type":"application/json"}

        return requests.get(URL,headers=HEADERS).json()


    def parse_data(self, data):
        return {
            "SharesOutstandingPCT":data['data']['ownershipSummary']['SharesOutstandingPCT']['value'],
            "ShareoutstandingTotal":data['data']['ownershipSummary']['ShareoutstandingTotal']['value'],
            "TotalHoldingsValue":data['data']['ownershipSummary']['TotalHoldingsValue']['value'],
            "IncreasedPositionsHolders":data['data']['activePositions']['rows'][0]['holders'],
            "IncreasedPositionsShares":data['data']['activePositions']['rows'][0]['shares'],
            "DecreasedPositionsHolders":data['data']['activePositions']['rows'][1]['holders'],
            "DecreasedPositionsShares":data['data']['activePositions']['rows'][1]['shares'],
            "HeldPositionsHolders":data['data']['activePositions']['rows'][2]['holders'],
            "HeldPositionsShares":data['data']['activePositions']['rows'][2]['shares'],
            "TotalInstitutionalHolders":data['data']['activePositions']['rows'][3]['holders'],
            "TotalInstitutionalShares":data['data']['activePositions']['rows'][3]['shares'],
            "NewPositionsHolders":data['data']['newSoldOutPositions']['rows'][0]['holders'],
            "NewPositionsShares":data['data']['newSoldOutPositions']['rows'][0]['shares'],
            "SoldOutPositionsHolders":data['data']['newSoldOutPositions']['rows'][1]['holders'],
            "SoldOutPositionsShares":data['data']['newSoldOutPositions']['rows'][1]['shares'],
        }


    def main(self):
        print(len(self.tickers))
        for i,tick in enumerate(self.tickers[0:1]):
            try:    
                print(tick)
                data = self.parse_data(self.get_data(tick))
                print(data)
            except Exception as e:
                self.errors_dicts_list.append({
                    'tick':tick, 'position':i, 'error':str(e)
                })
        pd.DataFrame(self.errors_dicts_list).to_csv()


if __name__ == '__main__':
    sc = Scarper()
    sc.main()
