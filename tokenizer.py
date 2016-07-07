from gensim.utils import smart_open, simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
import re
from stemming.porter2 import stem
from collections import defaultdict
from itertools import tee, islice, izip


class Tokenizer:
    
    def __init__(self, text):
        self.text = '"' + text + '"'

    def is_good(self, word):
        return (word not in STOPWORDS) and (1 < len(word) < 16)

    def gen_n_grams(self, n=2):
        n_gram_count = defaultdict(int)
        for phrase in re.split('. , ; :', self.text):
            tokens = self.gen_tokens(phrase)
            for n_gram in izip(*[islice(seq, i, None) for i, seq in enumerate(tee(tokens, n))]):
                yield n_gram

    def get_n_gram_count(self, n=2, join=False):
        n_gram_count = defaultdict(int)
        for n_gram in self.gen_n_grams(n):
            n_gram = '.'.join(n_gram) if join else n_gram
            n_gram_count[n_gram] += 1
        return n_gram_count
    
    def get_token_count(self):
        token_count = defaultdict(int)
        for tok in self.gen_tokens(self.text):
            token_count[tok] += 1
        return token_count

    def gen_tokens(self, string=None):
        string = string if string else self.text
        for word in simple_preprocess(string):
            if self.is_good(word):
                yield stem(word) 