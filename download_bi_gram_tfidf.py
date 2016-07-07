import pandas as pd
import math
from pymongo import MongoClient
import pymysql
import sys
from collections import defaultdict
import os
import csv
from features import Features


def get_remote_db_context():
    return pymysql.connect(user='admin', password='tr001', host='ECSC00104617.epam.com', database='tr_news_max_challenge')

    
csv_path, sep = 'bg_tfidf_data.csv', '\t'

cnx =  get_remote_db_context()
select_cur = cnx.cursor()
data_query = '''

select _guid, body 
from labeled
;
'''
test_ids_query = 'select id from test_ids;'

select_cur.execute(test_ids_query)

print 'fetching data...',
df = pd.read_sql(data_query, cnx)
test_ids = set(map(lambda i: i[0], select_cur.fetchall()))
print 'done'
cnx.close()

df = Features(df).add_tfidf()
df.drop('body', axis=1, inplace=True)
mask = df['_guid'].apply(lambda i: i in test_ids)
df_test = df[mask]
df_train = df[~mask]

file_name, ext = os.path.splitext(csv_path)
df_test.to_csv(file_name + '_test' + ext, sep=sep, quoting=csv.QUOTE_ALL)
df_train.to_csv(file_name + '_train' + ext, sep=sep, quoting=csv.QUOTE_ALL)