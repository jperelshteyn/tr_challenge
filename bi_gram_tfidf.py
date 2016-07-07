from pymongo import MongoClient
import pickle
from collections import Counter
import os


N = 4255+42735+31393+99681
lo = .01 * N
hi = .7 * N

to_delete = list()

bi_gram_freqs = pickle.load(open('data\\pickles\\bi_gram_freqs_complete.pkl', "r" ))

for bg, count in bi_gram_freqs.iteritems():
    if count < lo or count > hi:
        to_delete.append(bg)
        
for bg in to_delete:
    del bi_gram_freqs[bg]  
    
    
ids = set()

for g1, g2 in bi_gram_freqs.iterkeys():
    ids.add(g1)
    ids.add(g2)
    
client = MongoClient('ECSC00104617.epam.com:27017')
mdb = client.news_tfidf
token_dict = {rec['i']: rec['t'] 
              for rec in mdb.token_corpus_freqs.find({'i': {'$in': list(ids)}})}

bi_tok_freqs = list()

for bg, count in bi_gram_freqs.iteritems():
    bi_gram = token_dict[bg[0]] + '.' + token_dict[bg[1]]
    bi_tok_freqs.append((bi_gram, count,))