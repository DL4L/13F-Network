
# coding: utf-8

# In[ ]:

from yahoo_fin import stock_info
import pandas as pd
import numpy as np
from bisect import bisect_left, bisect_right
import datetime  
from pytz import timezone
import pickle
import json


# In[ ]:

def create_price_table(tickers, start_date = '2019-01-01', end_date = datetime.datetime.now().strftime('%Y-%m-%d')):
    
    price_table = pd.DataFrame()
    
    for ticker in tickers:
        try:
            price = stock_info.get_data(ticker, start_date, end_date)
        except:
            continue
        
        if price_table.empty:
            price_table = price.pivot(columns='ticker',values='adjclose')
        else:
            price_table = price_table.join(price.pivot(columns='ticker',values='adjclose'))
    
    return price_table
    
def format_price_table(price_data):
    """
    returns price data which has a price for end date
    """
    notnull = price_data.iloc[-1].notnull() # Which columns have final row null
    price_data_not_null = price_data[[i[0] for i in notnull.items() if i[1]]]
    price_data_not_null.columns.name = 'ticker'
    return price_data_not_null

def format_price_table_nan_values(price_data, thresh=0.1):
    
    drop_stocks = price_data.isna().apply('mean').reset_index().rename(columns = {0:'nas'}).query('nas > %s'%(thresh))
    price_data = price_data.loc[:,~price_data.columns.isin(drop_stocks['ticker'])]
    return price_data
def get_stdevs_trim_price_data(price_data_not_null,upper_bound=0.95,lower_bound=0.05):
    
    """
    calculates std/mean of returns
    trims according to bounds
    returns std/mean table and trimmed price data
    """
    stdevs = price_data_not_null.apply(lambda x: np.std(x)/x.mean())
    keep = stdevs[(stdevs<stdevs.quantile(upper_bound)) & (stdevs>stdevs.quantile(lower_bound))].index
    price_data_not_null = price_data_not_null[keep] # Trim price data according to std bounds
    
    return stdevs,price_data_not_null

def get_correlation_table(price_data):
    
    price_data_pct_change = price_data.pct_change(fill_method='ffill')[1:]
    df_cor = price_data_pct_change.corr().reset_index().melt(id_vars='ticker', var_name='cor').query('ticker != cor')
    #Corr table has duplicates
    df_cor = df_cor[pd.DataFrame(np.sort(df_cor[['ticker','cor']].values,1)).duplicated().values] 
    df_cor = df_cor.rename(columns={'ticker':'ticker1', 'cor':'ticker2','value':'cor'})
    
    return df_cor

def trim_cor_table(df_cor, upper_bound=0.9,lower_bound=0.5):
    
    df_cor_trimmed = df_cor[(df_cor['cor']<upper_bound) & (df_cor['cor'] > lower_bound)]
    return df_cor_trimmed

