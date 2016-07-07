import pandas as pd
import math
from pymongo import MongoClient
import pymysql
import sys
from collections import defaultdict
import os
import csv

client = MongoClient('ECSC00104617.epam.com:27017')
mdb = client.news_tfidf

N = 1337790
lo_freq = 0.01 * N
hi_freq = 0.7 * N

doc_freqs = {rec['i']: rec for rec in mdb.token_corpus_freqs.find(
            {
                '$and': [
                        {'c': {'$gte': lo_freq}},
                        {'c': {'$lte': hi_freq}}
                    ]
            }
        )}

relevant_tokens = set()

def get_remote_db_context():
    return pymysql.connect(user='admin', password='tr001', host='ECSC00104617.epam.com', database='tr_news_max_challenge')

def calc_tfidf(token):
    relevant_tokens.add(token['i'])
    term_freq = token['c']
    doc_freq = doc_freqs[token['i']]['c']
    return (1 + math.log(term_freq)) * math.log(1 + N / doc_freq)

def get_tfidf_scores(ids):
    sql_tbl_id, sql_id = ids
    record = mdb.doc_freq.find_one({'sql_id': int(sql_id), 'sql_tbl_id': sql_tbl_id})
    tfidf_dict = defaultdict(int)
    if record:
        tfidf_dict.update({token['i']: calc_tfidf(token) 
                     for token in record['t_counts'] if token['i'] in doc_freqs})
    return tfidf_dict
    
def main(args):
    
    csv_path, sep = 'tfidf_data.csv', '\t'

    if len(args) == 2:
        arg1, arg2 = args
        csv_path, sep = (arg1, arg2) if len(arg1) > len(arg2) else (arg2, arg1)
    elif len(args) == 1:
        arg = args.pop()
        if len(arg) < 3:
            sep = arg
        else:
            csv_path = arg
    
    
    cnx =  get_remote_db_context()
    select_cur = cnx.cursor()
    data_query = '''
    
    select _data_source, _id, _guid
        ,significance, title
        -- pub_date, journal_code, orgs, persons, topics, body 
    from labeled
    ;
    '''
    test_ids_query = 'select id from test_ids;'
    
    select_cur.execute(test_ids_query)

    print 'fetching data...'
    df = pd.read_sql(data_query, cnx)
    test_ids = set(map(lambda i: i[0], select_cur.fetchall()))
    
    cnx.close()
    
    print 'calculating tfidf...'
    tfidf = map(get_tfidf_scores, zip(df['_data_source'], df['_id']))
            
    print 'adding tfidf...'
    for token_id, token in doc_freqs.iteritems():
        
        if token_id in relevant_tokens:
            
            df[token['t']] = [freq[token_id] for freq in tfidf]
            

    mask = df['_guid'].apply(lambda i: i in test_ids)
    df_test = df[mask]
    df_train = df[~mask]

    file_name, ext = os.path.splitext(csv_path)
    df_test.to_csv(file_name + '_test' + ext, sep=sep, quoting=csv.QUOTE_ALL)
    df_train.to_csv(file_name + '_train' + ext, sep=sep, quoting=csv.QUOTE_ALL)
      

if __name__ == "__main__":
    main(sys.argv[1:])