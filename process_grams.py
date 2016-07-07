import time
import pymysql
from collections import defaultdict, Counter
import json
import io
from pymongo import MongoClient, ASCENDING, HASHED
from tokenizer import Tokenizer



def get_db_context():
    return pymysql.connect(user='admin', password='tr001', host='localhost', database='tr_news_max_challenge')

def gen_ids():
    i = 0
    while True:
        i += 1
        yield i

        
def insert_bi_gram_freqs(db, source_id, bi_gram_freqs):
    if bi_gram_freqs:
        db.bi_gram_corpus_freqs.insert_many(bi_gram_freqs.values())



def refresh_token_freqs(db, token_freqs):
    db.token_corpus_freqs.drop()
    db.token_corpus_freqs.insert_many(token_freqs.values())
          

def get_token_dict(db):
    return {t['t']: t['i'] for t in db.token_corpus_freqs.find()}


def extract_grams():

    client = MongoClient()
    db = client.news_tfidf

    select_strs = [
        # ("se" ,"select ID, story from swift_excel_articles;"),
        # ("ne", "select ID, body from nsc_excel_articles;"),
        ("sw", "select sw.ID, sw.story from swift_articles sw left join swift_excel_articles se on sw.ID = se.swiftID where se.swiftID is null;"),
        ("ns", "select ns.ID, concat(ns.lead_parag, ' ', ns.body) from nsc_articles ns left join nsc_excel_articles ne on ns.ID = ne.nscID where ne.nscID  is null;")
    ]
    
    id_gen = gen_ids()

    token_ids = get_token_dict(db)

    # token_freqs = dict()
    bi_gram_freqs = dict()
    
    doc_counts = list()
    
    t = time.time()

    row_count = 0
    cnx =  get_db_context()
    
    def incr_token_dict(t_dict, token):
        if token in t_dict:
            t_dict[token]['c'] += 1
        else:
            t_dict[token] = {'i': next(id_gen), 't': token, 'c': 1}

    
    def incr_bi_gram_dict(bg_dict, bg, source_id=None):
        if bg in bg_dict:
            bg_dict[bg]['c'] += 1
        else:
            if source_id:
                bg_dict[bg] = {'g1': bg[0], 'g2': bg[1], 'c': 1, 'source_id': source_id}
            else:
                bg_dict[bg] = {'g1': bg[0], 'g2': bg[1], 'c': 1}

    
    for source_id, query in select_strs:

        print query

        cnx =  get_db_context()
        select_cur = cnx.cursor()
        select_cur.execute(query)
        
        for article_id, article, in select_cur:

            row_count += 1
            if row_count % 5000 == 0:
                print 'processed', row_count, 'rows in', (time.time() - t) / 60, 'minutes'
                t = time.time()

            if type(article) in (str, unicode) and len(article) > 0:
            
                tokenizer = Tokenizer(article)
                # token_gen = tokenizer.gen_tokens()
                bi_gram_gen = tokenizer.gen_n_grams(n=2)
                
                # token_doc_freqs = dict()
                bi_gram_doc_freqs = dict()
                
                # for token in token_gen:
                    
                #     incr_token_dict(token_freqs, token)
                #     incr_token_dict(token_doc_freqs, token)

                for bi_gram in bi_gram_gen:
                    
                    gram1_id = token_ids[bi_gram[0]]
                    gram2_id = token_ids[bi_gram[1]]

                    incr_bi_gram_dict(bi_gram_doc_freqs, (gram1_id, gram2_id))
                    incr_bi_gram_dict(bi_gram_freqs, (gram1_id, gram2_id), source_id)
                    
                doc_counts.append(
                {
                        'sql_id': article_id,
                        'sql_tbl_id': source_id,
                        # 't_counts': token_doc_freqs.values(),
                        'bg_counts': bi_gram_doc_freqs.values()
                    }
                )
                
                if len(doc_counts) > 1000:
                    db_t = time.time()
                    print 'updating db...'
                    db.doc_freq.insert_many(doc_counts)
                    doc_counts = list()
                    
#                         update_doc_bg_counts(db, doc_counts)
#                         doc_counts = list()        

                    insert_bi_gram_freqs(db, source_id, bi_gram_freqs)
                    bi_gram_freqs = dict()

                    print 'db done in', (time.time() - db_t)/60, 'minutes'
                    
        # refresh_token_freqs(db, token_freqs)
        insert_bi_gram_freqs(db, source_id, bi_gram_freqs)
        bi_gram_freqs = dict()

        select_cur.close()
        cnx.close()      

    print 'parsing time: ', time.time() - t
    t = time.time()
    print 'updating...'
    
    db.doc_freq.insert_many(doc_counts)
#         update_doc_bg_counts(db, doc_counts)
    insert_bi_gram_freqs(db, source_id, bi_gram_freqs)




if __name__ == "__main__":
    extract_grams() 