import pandas as pd
import multiprocessing
import time
import gspread
import deep_stocks
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./original-advice-385307-e221975bf7db.json', scope)
client = gspread.authorize(creds)
gs = client.open('AllStockData')
main_sheet=gs.worksheet('Main')
all_record_main=main_sheet.get_all_records()
main_df=pd.DataFrame(all_record_main)
url_list=list(main_df['URL'])
def process_url(url):
    return deep_stocks.deep_stocks(url)
if __name__ == '__main__':
    url_list = url_list[1500:]  
    pool = multiprocessing.Pool()
    results = pool.map(process_url, url_list)
    pool.close()
    pool.join()
    result_df = pd.concat(results, ignore_index=True)
gsnew = client.open('StockPartion')
main_sheet=gsnew.worksheet('Part3')
main_sheet.clear()
set_with_dataframe(main_sheet,result_df)
