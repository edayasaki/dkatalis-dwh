import pandas as pd 
import numpy as np 
import os
import json 
import glob
import webbrowser

def df_load(path):
    path_list   = glob.glob(os.path.join(path,'*.json'))
    df          = pd.DataFrame()
    for file_path in path_list:
        dl  = json.load(open(file_path))
        dn  = pd.json_normalize(dl)    
        df  = df.append(dn)    
    df  = df.sort_values('ts')
    return df   

def df_set(df,col):
    set_col             ='set.' + col
    data_col            ='data.' + col    
    if data_col not in df:
        df[data_col]    = np.nan  
    df[data_col]        = np.where(df[set_col].notnull(), df[set_col], df[data_col])
    return df    

def df_update(df):
    for col in df.columns:
        if col[:3]=='set':
            column = col[4:] 
            df_set(df,column)
    return df

def df_table(path):
    df              = df_load(path)
    df              = df_update(df)   
    df.fillna(method='ffill',inplace=True) 
    df['datetime']  = df['ts'].values.astype(dtype='datetime64[ms]')       
    return df

## parameter
project_dir                     = os.path.abspath(__file__ + '/../../')
accounts_dir                    = project_dir + '/data/accounts/'
cards_dir                       = project_dir + '/data/cards/'
savings_dir                     = project_dir + '/data/savings_accounts/'
hide_dimension                  = pd.options.display.show_dimensions = False

## get data foreach datasets
df_accounts                     = df_table(accounts_dir)
df_cards                        = df_table(cards_dir)
df_savings                      = df_table(savings_dir)

## main process
df_cards['data.credit_used']    = df_cards['data.credit_used'].replace(0, np.nan)
df_cards['prev_credit_used']    = df_cards['data.credit_used'].shift(1).fillna(0)
df_cards['transaction']         = df_cards['data.credit_used'] - df_cards['prev_credit_used']
df_cards                        = df_cards.drop('prev_credit_used',1)

df_savings['prev_balance']      = df_savings['data.balance'].shift(1).fillna(0)
df_savings['transaction']       = df_savings['data.balance'] - df_savings['prev_balance']
df_savings                      = df_savings.drop('prev_balance',1)

df_as                           = df_accounts.loc[df_accounts['data.savings_account_id'].notnull()].tail(1)
df_as                           = df_as.drop('set.card_id',1)
df_as                           = df_as.drop('data.card_id',1)
df_as                           = pd.merge(df_as,df_savings,on='data.savings_account_id')
            
df_ac                           = df_accounts.replace('', np.nan)
df_ac                           = df_ac.loc[df_ac['data.card_id'].notnull()]
df_ac                           = df_ac.drop('set.savings_account_id',1)
df_ac                           = df_ac.drop('data.savings_account_id',1)
df_ac                           = pd.merge(df_ac,df_cards,on='data.card_id')

df_hist                         = df_ac.append(df_as,ignore_index=True)
df_hist                         = df_hist.sort_values('ts_y')
df_hist['transaction']          = df_hist['transaction'].replace(np.nan,0)
df_hist['account_number']       = np.where(df_hist['data.savings_account_id'].notnull(),df_hist['data.savings_account_id'],df_hist['data.card_id'])
df_hist['account_number']       = df_hist['data.account_id'] + '-' + df_hist['account_number']
df_hist['account_type']         = np.where(df_hist['data.savings_account_id'].notnull(), 'savings', 'credit card')

df_datetime                     = df_hist.pop('datetime_y') 
df_txn                          = df_hist.pop('transaction') 
df_hist['datetime']             = df_datetime 
df_hist['transaction']          = df_txn
df_txn                          = df_hist[['datetime','account_number','account_type','transaction']].copy().replace(0,np.nan).dropna()

## reset indices foreach datasets
df_accounts                     = df_accounts.reset_index(drop=True)
df_cards                        = df_cards.reset_index(drop=True)
df_savings                      = df_savings.reset_index(drop=True)
df_hist                         = df_hist.reset_index(drop=True)
df_txn                          = df_txn.reset_index(drop=True)

## reset indices foreach datasets
df_accounts                     = df_accounts.style.set_properties(**{'text-align': 'left'})
df_cards                        = df_cards.style.set_properties(**{'text-align': 'left'})
df_savings                      = df_savings.style.set_properties(**{'text-align': 'left'})
df_hist                         = df_hist.style.set_properties(**{'text-align': 'left'})
df_txn                          = df_txn.style.set_properties(**{'text-align': 'left'})

## Print results to console
print('Accounts Table:')
print(df_accounts)
print('')

print('Cards Table:')
print(df_cards)
print('')

print('Savings Table:')
print(df_savings)
print('')

print('History Table:')
print(df_hist)
print('')

print('Transaction Table:')
print(df_txn)
print('')