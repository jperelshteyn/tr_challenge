import pandas as pd
import math
import os
from collections import defaultdict
import pickle
from tokenizer import Tokenizer
from pymongo import MongoClient
import pickle
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer


client = MongoClient('ECSC00104617.epam.com:27017')
mdb = client.news_tfidf

def get_token_dict():
    token_dict = None
    if os.path.exists('pickles\\token_dict.pkl'):
        token_dict = pickle.load(open('pickles\\token_dict.pkl', 'r'))
    else:
        token_dict = {t['i']: t['t'] for t in mdb.token_corpus_freqs.find()}
        pickle.dump(token_dict, open('pickles\\token_dict.pkl', 'wb'))
    return token_dict

def get_freqs(sql_tbl_id, sql_id, token_dict):
    record = mdb.doc_freq.find_one({'sql_id': int(sql_id), 'sql_tbl_id': sql_tbl_id})
    token_freqs = defaultdict(int)
    if record:
        token_freqs.update({token_dict[token['i']]: token['c']
                     for token in record['t_counts']})
    bg_freqs = defaultdict(int)
    if record:
        bg_freqs.update({token_dict[bg['g1']]+'.'+token_dict[bg['g2']]: bg['c']
                     for bg in record['bg_counts']})
    return token_freqs, bg_freqs

class TFIDF:
    
    def __init__(self, term_type):
        assert term_type in ('token', 'bi_gram')

        self.N = 1337790 if term_type == 'token' else 178064
        self.terms, self.term_doc_freqs = self.get_term_info(term_type)
        
    def get_term_info(self, term_type):
        terms = list()
        doc_freqs = dict()
        root_path = 'C:\\tr_data_challenge_api\\data_model_code\\file_parser\\'
        path = root_path + 'data\\{}_freqs_200.pkl' if term_type == 'bi_gram' else root_path + 'data\\{}_freqs.pkl'
        for term, freq in pickle.load(open(path.format(term_type), "r" )): 
            terms.append(term)
            doc_freqs[term] = freq
        return terms, doc_freqs
    
    def calc_tfidf(self, term_freq, doc_freq):
        if term_freq == 0:
            return 0
        return (1 + math.log(term_freq)) * math.log(1 + self.N / doc_freq)
    
    def get_tfidf(self, term_counts):
        assert type(term_counts) is defaultdict

        return [self.calc_tfidf(term_counts[term], self.term_doc_freqs[term]) 
                                for term in self.terms]
    
class Features:
    
    def __init__(self, df):
        self.df = df
        self.df['body'] = self.df['lead parag']+' '+self.df['story body'] if 'story body' in self.df.columns else self.df['body']
        
    def add_tfidf(self):
        
        print 'calculating tfidf...'

        token_tfidf = TFIDF('token')
        bi_gram_tfidf = TFIDF('bi_gram')
        
        texts = self.df['body']
        
        # tfidf_df = pd.DataFrame(columns=token_tfidf.terms)
        tfidf_df = pd.DataFrame(columns=token_tfidf.terms + bi_gram_tfidf.terms)
        
        is_from_sql = ('_data_source' in self.df.columns)
        token_dict = get_token_dict() if is_from_sql else None

        count = 0
        for i, text in enumerate(texts):

            if i > 0 and i % 500 == 0:
                print i, 'rows'

            if text:
                token_freqs = None
                bi_gram_freqs = None
                
                if is_from_sql:
                    sql_tbl_id, sql_id = self.df._data_source[i], self.df._id[i]
                    token_freqs, bi_gram_freqs = get_freqs(sql_tbl_id, sql_id, token_dict)
                else:
                    tokenizer = Tokenizer(text)
                    token_freqs = tokenizer.get_token_count()
                    bi_gram_freqs = tokenizer.get_n_gram_count(2, True)
                
                row = token_tfidf.get_tfidf(token_freqs) + bi_gram_tfidf.get_tfidf(bi_gram_freqs)
                tfidf_df.loc[i] = row
                # tfidf_df.loc[i] = token_tfidf.get_tfidf(token_freqs)
                count = i+1
        self.df = pd.concat([self.df, tfidf_df], axis=1)
        print "tfidf'd", count, 'rows' 

    def add_field(self, field, new_field, transformer):
        self.df[new_field] = self.df[field].apply(transformer)

    def add_field_count(self, field, new_field, sep=None): 
        count = lambda val: len(unicode(val).split(sep) if sep else val) if not pd.isnull(val) else 0
        self.add_field(field, new_field, count)
        
    def add_count_feats(self):
        print 'Adding counts...',
        self.add_field_count('story body', 'story_word_count', ' ')
        self.add_field_count('RCS', 'topics_count', ' ')
        self.add_field_count('title', 'title_word_count', ' ')
        self.add_field_count('OA perm ids', 'org_count', ' ')
        self.add_field('story body', 'story.length', lambda s: len(s))
        print 'done'
        
    def add_sentiment(self):
        print 'Adding sentiment...',
        sia = SentimentIntensityAnalyzer()
        for sentiment in ('pos', 'neg', 'neu', 'compound'):
            sentify = lambda s: sia.polarity_scores(s[:200])[sentiment]
            self.df['sentiment_' + sentiment] = self.df['story body'].apply(sentify)
        print 'done'
            
    def add_pattern_counts(self):
        print 'Adding pattern counts...',
        patterns = Patterns()
        for pattern_name in patterns.names:
            get_count = lambda s: patterns.get_count(pattern_name, s)
            self.df[pattern_name] = self.df['body'].apply(get_count)
        print 'done'
        
    def normalize(self, X, mean, std):
        return (X - mean) / std
    
    def normalize_column(self, column):
        mean, std = self.df[column].mean(), self.df[column].std()
        self.df[column] = self.normalize(self.df[column], mean, std)
    
    def normalize_training(self):
        columns = ['story_word_count', 'topics_count', 'title_word_count', 'org_count', 'story.length']
        for column in columns:
            self.normalize_column(column)
            
    def normalize_new_data(self):
        norm_values = {
            'story.length': {'mean': 4645.554129, 'std': 6796.77025},
            'numeric.percent': {'mean': 8.182659, 'std': 57.78209},
            'numeric.comma': {'mean': 4.401744, 'std': 29.81231}
        }
        
        print 'Normalizing...',
        for column in norm_values:
            mean, std = norm_values[column]['mean'], norm_values[column]['std']
            self.df[column] = self.normalize(self.df[column], mean, std) 
        print 'done'
        
    def clean(self):
        shorten_body = lambda b: b if len(b) < 1001 else b[:1000] + '...'
        self.df['body'] = self.df['body'].apply(shorten_body)
        self.df.drop([#'_guid', 
                        'lead parag', 'RICs', 'source', 
                      'person perm ids', 'story body'], axis=1, inplace=True)
        self.df.rename(columns = {'story date': 'date.time', 'journal code': 'journal.code', 
                                  'OA perm ids': 'organizations.codes', 'RCS':'topics.codes',
                                 'body': 'story.body', 'title': 'story.title'}, inplace=True)
        
class Patterns:
    
    def __init__(self):
        self.names = ['numeric.percent', 'numeric.dollar.illi', 'numeric.comma']
        self.patterns = {'numeric.percent': re.compile(r'\d+\.?\d* ?%'),
                        'numeric.dollar.illi': re.compile(r'\$\d+\.?\d*.{0,6}illi'),
                        'numeric.comma': re.compile(r'[^\$]\d+,\d+')}
        self.idfs = {'numeric.percent': 0.9554014,
                    'numeric.dollar.illi': 1.2646519,
                    'numeric.comma': 0.8583480}
               
    def get_count(self, pattern_name, text):
        pattern = self.patterns[pattern_name]
        idf = self.idfs[pattern_name]
        return len(re.findall(pattern, text)) * idf
        
    