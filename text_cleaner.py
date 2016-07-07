import re
from mysql import connector
from string_cleaner import StringCleaner

def get_db_context():
    return connector.connect(user='admin', password='tr001', host='localhost', database='tr_news_max_challenge')


def clean_db_text(table, id_col, text_cols):


    cnx =  get_db_context()
    cursor = cnx.cursor()
    updt_cursor = cnx.cursor()

    try:
        query = ("select {}, {} from {};".format(id_col, ','.join(text_cols), table))

        print query
        
        cursor.execute(query)
        
        cleaner = StringCleaner()
            
        c = 0
        
        for result in cursor:
        
            c += 1
            if c % 10000 == 0:
                print c, 'rows'
        
            row_id, texts = result[0], result[1:]

            for col_order, text in enumerate(texts):

                need_update, clean_text = cleaner.clean(text)

                if need_update:

                    update = ("update {} set {} = %s where {} = %s;".format(table, text_cols[col_order], id_col ))
                    updt_cursor.execute(update, (clean_text, row_id))
                    cnx.commit()
    finally:
        cnx.close()

def main():
    clean_db_text('swift_articles', 'ID', ['story', 'subheadline'])
    clean_db_text('swift_excel_articles', 'ID', ['story', 'subheadline'])

if __name__ == "__main__":
    main() 