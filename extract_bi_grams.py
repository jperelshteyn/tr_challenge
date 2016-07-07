import time
import pymysql
from collections import defaultdict, Counter
import json
import io
from pymongo import MongoClient, ASCENDING, HASHED
from tokenizer import Tokenizer
import pickle
import os

def get_db_context():
    return pymysql.connect(user='admin', password='tr001', host='localhost', database='tr_news_max_challenge')

def get_local_db_context():
    return pymysql.connect(user='root', password='iw3ugwiz', host='localhost', database='tr_news_max_challenge')

def get_remote_db_context():
    return pymysql.connect(user='admin', password='tr001', host='ECSC00104617.epam.com', database='tr_news_max_challenge')

def insert_bi_gram_freqs(db, source_id, bi_gram_freqs):
    if bi_gram_freqs:
        db.bi_gram_corpus_freqs.insert_many(bi_gram_freqs.values())


def get_token_dict(db):
    return {t['t']: t['i'] for t in db.token_corpus_freqs.find()}

def delete_low_counts(counter):
    print 'deleting...',
    to_delete = [key for key, count in counter.iteritems() if count < 5]
    for key in to_delete:
        del counter[key]
    print 'done'

def pickle_bi_gram_freqs(bi_gram_freqs, source_id):
    if not os.path.exists("pickles"):
        os.makedirs("pickles")
    part = 1
    fname = 'pickles//bi_gram_freqs_{}_{}.pkl'.format(source_id, part)
    while os.path.exists(fname):
        part += 1
        fname = 'pickles//bi_gram_freqs_{}_{}.pkl'.format(source_id, part)
    pickle.dump(bi_gram_freqs, open(fname, "wb" ) )
    
def extract_grams():

    client = MongoClient()
    db = client.news_tfidf

    select_strs = [
        ("se" ,"select ID, story from swift_excel_articles;"),
        ("ne", "select ID, body from nsc_excel_articles;"),
        ("sw", '''select sw.ID, sw.story 
                from swift_articles sw left join swift_excel_articles se on sw.ID = se.swiftID 
                where se.swiftID is null and in_sample=1;'''),
        ("ns", '''select ns.ID, concat(ns.lead_parag, ' ', ns.body) 
                from nsc_articles ns left join nsc_excel_articles ne on ns.ID = ne.nscID 
                where ne.nscID is null and in_sample=1;''')
    ]

    token_ids = get_token_dict(db)
    bi_gram_freqs = defaultdict(int)
    
    t = time.time()
    st = time.time()
    row_count = 0
    
    for source_id, query in select_strs:

        print query

        cnx = get_db_context()
        select_cur = cnx.cursor()
        select_cur.execute(query)
        
        for article_id, article, in select_cur:

            row_count += 1
            if row_count % 10000 == 0:
                print 'processed', row_count, 'rows in', (time.time() - t) / 60, 'minutes'
                t = time.time()

            if row_count % 10000 == 0:
                delete_low_counts(bi_gram_freqs)
                
            if type(article) in (str, unicode) and len(article) > 0:
            
                tokenizer = Tokenizer(article)
                bi_gram_gen = tokenizer.gen_n_grams(n=2)
                counted = set()
                
                for bi_gram in bi_gram_gen:
                    
                    bg_id_tup = (token_ids[bi_gram[0]], token_ids[bi_gram[1]])
                    
                    if bg_id_tup not in counted:
                        bi_gram_freqs[bg_id_tup] += 1
                        counted.add(bg_id_tup)

            if len(bi_gram_freqs) > 1000000:
                pickle_bi_gram_freqs(bi_gram_freqs, source_id)
                bi_gram_freqs = defaultdict(int)                
                        
        pickle_bi_gram_freqs(bi_gram_freqs, source_id)
        bi_gram_freqs = defaultdict(int)
        select_cur.close()
        cnx.close()      

    print 'parsing time: ', (time.time() - st) / 60, 'minutes'
    return bi_gram_freqs

      
if __name__ == "__main__":
    extract_grams()