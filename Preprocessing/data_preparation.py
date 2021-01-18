
import urllib.request, json 
import json
import urllib3
import numpy as np
import pandas as pd

import mysql.connector
import pymysql
from sqlalchemy import create_engine
from pandas.io import sql



### configs ### 

companies = ['EBAY']

user = 'root'
passw = 'B@ldoo2019'
host =  '127.0.0.1'
port = 3306
database = 'news_final'
price_api_key ='RDJGGT2IBZO1891D'
stock_api_key = 'uwleibwkfzddwxgmbbmal41bkawkbtqjtk1gakmp'

### creating df of alpha ### 

for company_name in companies:

    print("1")
    query = 'https://www.alphavantage.co/query?&datatype=json&function=TIME_SERIES_DAILY&outputsize=full&symbol=' + company_name + '&apikey=' + price_api_key

    with urllib.request.urlopen(query) as url:
        data = json.loads(url.read().decode())
        data = data['Time Series (Daily)']

    print("2")

    df = pd.DataFrame.from_dict(data).transpose()
    df.rename(columns={'1. open':'open',
                              '2. high':'high',
                              '3. low':'low',
                              '4. close':'close',
                              '5. volume':'volume'}, 
                              inplace=True)

    print("3")
    df['date'] = df.index

    #if conn.open() == False:
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passw)
    conn.cursor().execute("CREATE DATABASE IF NOT EXISTS {0} ".format(database))

    #1. create the fucking database 
    engine = create_engine('mysql+pymysql://root:B@ldoo2019@localhost/'+database)
    connect = engine.connect()

    #2. create the fucking table 
    df.to_sql(name=company_name, con=connect, if_exists = 'replace', index=False)#, flavor = 'mysql')

    #A = find the min and max date of the database.table

    engine.execute('SET @date_min = (select min(date) from ' + database + '.' + company_name + ');')
    engine.execute('SET @date_max = (select max(date) from ' + database + '.' + company_name + ');')

    #B = create a new table with dates and stock prices as well as null prices 

    engine.execute('CREATE TABLE IF NOT EXISTS '+company_name+'_PROCESSED AS (WITH d AS (SELECT * from (select DATE_ADD(@date_max, INTERVAL (@i:=@i-1) DAY) as `datefield` from '+company_name+', (SELECT @i:=0) gen_sub where DATE_ADD(@date_min,INTERVAL -@i DAY) BETWEEN @date_min AND @date_max) date_generator LEFT OUTER JOIN '+company_name+' on '+company_name+'.date = date_generator.datefield ORDER BY date_generator.datefield DESC) SELECT * FROM d);')

    #C = retrieve the table we need from the sql 
    cursor = conn.cursor()
    cursor.execute('SELECT datefield, close from ' + database + '.'+company_name+'_PROCESSED order by datefield desc;')
    records = cursor.fetchall()

    #print(list(records))
    df = pd.DataFrame(records, columns =['date', 'price']) 
    df["price"] = pd.to_numeric(df["price"])
    df = df.fillna(value=np.nan)
    print(df)
    df = df.interpolate(method='linear', limit_direction='forward')

    print(df.shape[0])

    labels = [0]*df.shape[0]
    df['labels'] = labels
    print(df)

    def labelling(df, window_size, barriers):
        #start_index = window_size
        start_index = window_size
        while start_index < df.shape[0]:
            current_price = df['price'].iloc[start_index]
            
            range_start_idx = max(0, start_index-window_size)

            max_price = max(df['price'].iloc[range_start_idx:start_index])
            min_price = min(df['price'].iloc[range_start_idx:start_index])

            if  max_price - current_price >= current_price*barriers:
                df['labels'].iloc[start_index] = 1

            elif  current_price - min_price >= current_price*barriers:
                df['labels'].iloc[start_index] = -1

            else:
                df['labels'].iloc[start_index] = 0

            start_index = start_index + 1

    labelling(df, 3, 0.05)
    #print(Counter(df['labels']))
    d = {}
    for el in df['labels']:
        if el not in d:
            d[el] = 1
        else:
            d[el] += 1

    print(d)


    df.to_sql(name=company_name+'_FINAL_LABELS', con=connect, if_exists = 'replace', index=False)


    ##### stock_news

    
    page_size = 50
    page = 1
    name = 'EBAY'

    def returnPages(api_key, page_size, page, name):

        if name == 'all':
            query_dict = {'section': 'alltickers',
                                  'items':str(page_size),
                                  'token':stock_api_key,
                                 'page': str(page)}
            query = 'https://stocknewsapi.com/api/v1/category?'
            for key in list(query_dict.keys()):
                query = query + key + '=' + query_dict[key]+'&'
            query = query[0:-1]
        else:
            query_dict = {'tickers': name,
                              'items':str(page_size),
                              'token':stock_api_key,
                             'page': str(page)}
                #Assemble query:
            query = 'https://stocknewsapi.com/api/v1?'
            for key in list(query_dict.keys()):
                query = query + key + '=' + query_dict[key]+'&'
                    
            query = query[0:-1]

        print(query, "STOCKNEWS_API")
                
        with urllib.request.urlopen(query) as url:
            data = json.loads(url.read().decode())

        if data['data'] ==[]:
            pages = 0
        else:
            pages = data['total_pages']

        return data['data'], pages

    #page_number = 1000

    x, page = returnPages(stock_api_key, page_size, page, name)
    #page = 40
    page = 10
    final = []

    while page > 0:
    	x, _ = returnPages(stock_api_key, page_size, page, name)
    	final = final + x
    	page = page - 1

    print(final)
    print(len(final))

    df = pd.DataFrame.from_dict(final)
    print(df)
    print(df.columns)
    print(df.index)

    #df.to_csv(company_name+'_file.csv')
    df = df.drop(['topics','tickers'],axis=1)
    df['date'] = pd.to_datetime(df['date'],utc=True)
    print(pd.to_datetime(df['date']).iloc[0])



    #conn = pymysql.connect(host=host, port=port, user=user, passwd=passw)

    #conn.cursor().execute("CREATE DATABASE IF NOT EXISTS {0} ".format(database))

    #engine = create_engine('mysql+pymysql://root:B@ldoo2019@localhost/' + database)

    #connect = engine.connect()

    df.to_sql(name=company_name+'_text', con=connect, if_exists = 'replace', index=False)#, flavor = 'mysql')


    #cursor = conn.cursor()

    engine.execute('ALTER TABLE ' + database + '.'+company_name+'_text ADD (new_col DATE);') 

    engine.execute('UPDATE ' + database + '.'+company_name+'_text SET new_col=date(date);')

    #cursor.execute('UPDATE ' + database + '.TWTR_text SET new_col=date(date);')

    records = engine.execute('SELECT * FROM '+database+'.'+company_name+'_text LEFT JOIN '+database+'.'+company_name+'_FINAL_LABELS ON '+database+'.'+company_name+'_text.new_col = ' + database +'.'+company_name+'_FINAL_LABELS.date;')

    table1_columns = engine.execute('SHOW COLUMNS FROM '+company_name+'_text ')
    table2_columns = engine.execute('SHOW COLUMNS FROM '+company_name+'_FINAL_LABELS')


    table1_column_names = [arr[0] for arr in table1_columns]
    table2_column_names = [arr[0] for arr in table2_columns]

    print(table1_column_names, "EXIT")
    print(table2_column_names, "EXIT")

    all_columns = table1_column_names + table2_column_names

    print(all_columns)

    #print(table1_columns.fetchall())
    #print(table2_columns.fetchall())

    print('STARTED FETCHING')
    dataset_prepared = records.fetchall()

    print('CREATING PANDAS DATAFRAME')
    dataset_prepared = pd.DataFrame.from_dict(dataset_prepared)#, columns=all_columns)
    dataset_prepared.columns = all_columns
    print(dataset_prepared)


    #print(list(records))
    #df = pd.DataFrame(records, columns =['date', 'price']) 
    #print(df, "RECORDS")
    print('SAVING INTO CSV')
    dataset_prepared.to_csv(company_name+'_prepared.csv')
    print('AFTER INTO CSV')
    conn.close()
    connect.close()
    engine.dispose()



