from pymongo import MongoClient


def sum_bi_grams_counts():
    client = MongoClient()
    db = client.news_tfidf
    db.bi_gram_corpus_freqs.aggregate([
        {'$group': {'_id': {'g1': '$g1', 'g2': '$g2'}, 'count': {'$sum': 1}}},
        {'$out': 'agged_bi_gram_corpus_freqs'}
    ], 
                                         allowDiskUse=True
    )
      
if __name__ == "__main__":
    sum_bi_grams_counts()