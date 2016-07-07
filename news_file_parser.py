from lxml import etree
from shutil import rmtree
import sys
import io
import codecs
import json
import os
from string_cleaner import StringCleaner
from datetime import datetime
import pandas as pd
from features import Features
import csv
from collections import defaultdict


tag_cleaner = StringCleaner()


def convert_to_unix_time(date_string, doc_type):
    assert doc_type in ('xml', 'json')
    
    dt_format = '%Y%m%d%H%M%S' if doc_type == 'xml' else '%Y-%m-%dT%H:%M:%S'
    
    if doc_type == 'json':
        date_string = date_string.split('.')[0]
    
    dt = datetime.strptime(date_string, dt_format)

    return str(int((dt - datetime.utcfromtimestamp(0)).total_seconds()))

        
def clean_val(val):
    for cr in (('\r', u' '), ('\t', u' '), ('\n', u' '), ('"', u"'")):
        val = val.replace(cr[0], cr[1])
    return u'"' +  val + u'"'


class XMLParser:
    
    def __init__(self, root, xpath_map):
        assert type(root) == etree._Element
        assert root.tag == 'n-document'
        
        self.root = root
        
        self.fields = {field: root.xpath(path) 
                       for field, path in xpath_map.iteritems()}

        self.guid = self.get_val('guid')

    
    def to_list(self, cols):
        return [self.get_val(c, False) for c in cols]


    def to_csv_row(self, cols, sep):
        return sep.join(map(self.get_val, cols))
        

    def get_val(self, field, clean=True):

        val = self.fields[field]
        val = ' '.join(val) if type(val) is list else unicode(val)
        
        if field in ('lead parag', 'story body'):
            _, val = tag_cleaner.clean(val)
        
        if field == 'story date':
            val = convert_to_unix_time(val, 'xml')
            
        if clean:
            return clean_val(val)
        else:
            return val

    

class XMLFile:
    
    def __init__(self, xml_file_path):
        # self.tree = etree.parse(xml_file_path)
        
        self.iterator = etree.iterparse(xml_file_path, events=('end',), tag='n-document')
        
        self.cols = ['guid', 
                     'journal code',
                     'source', 
                     'story date', 
                     'RICs', 
                     'person perm ids', 
                     'OA perm ids', 
                     'RCS', 
                     'title', 
                     'lead parag',
                     'story body']
        
        self.xpath_map = {
            'guid': '@guid',
            'source': 'n-docbody/document/supplier-info/supplier-num/@nitf-regsrc',
            'story date': 'n-metadata/metadata.block/md.dates/md.publisheddatetime/text()',
            'RICs': 'n-docbody/document/indexing/extraction-terms/extr-company-block//ric/@norm',
            'person perm ids': 'n-docbody/document/indexing/extraction-terms/extr-person-block//person-perm-id/text()',
            'OA perm ids': 'n-docbody/document/indexing/extraction-terms/extr-company-block//norm-company/@oa-id',
            'RCS': 'n-docbody/document/indexing/classification-terms/rcs-block//rcs/text()',
            'title': 'n-metadata/metadata.block/md.descriptions/md.title/text()',
            'lead parag': 'n-docbody/document/content/text[@type="lead-para"]/descendant::*/text()',
            'story body': 'n-docbody/document/content/text[@type="body"]/descendant::*/text()',
            'journal code': 'n-docbody/document/pub-info/src-data/src-journal-code/text()'
        }
    
    
    def get_csv_header(self, sep):
        
        return sep.join(self.cols) + u'\n'

        
    def to_df(self, columns):

        df = pd.DataFrame(columns=columns)

        rec_count = 0
        error_count = 0  

        while True:

            try:
                _, elem = next(self.iterator)

            except StopIteration:
                break

            except etree.XMLSyntaxError:
                error_count += 1
                continue

            doc = XMLParser(elem, self.xpath_map)

            df.loc[rec_count] = doc.to_list(self.cols)

            rec_count += 1

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]  

        print 'Parsed', rec_count, 'rows with', error_count, 'errors'

        return df

    def to_csv(self, csv_file, sep):

        write_buffer = list()

        rec_count = 0
        error_count = 0

        write_to_file = lambda s: csv_file.write(unicode(s))

        while True:

            try:
                _, elem = next(self.iterator)

            except StopIteration:
                break

            except etree.XMLSyntaxError:
                error_count += 1
                continue

            doc = XMLParser(elem, self.xpath_map)
            rec_count += 1

            write_buffer.append(doc.to_csv_row(self.cols, sep))

            if len(write_buffer) > 10000:
                
                print 'List buffer size:', sys.getsizeof(write_buffer)
                
                write_to_file(u'\n'.join(write_buffer) + u'\n')                
                
                write_buffer = list()

            elem.clear()

            while elem.getprevious() is not None:
                del elem.getparent()[0]                    

        write_to_file(u'\n'.join(write_buffer) + u'\n') 

        print 'Saved', rec_count, 'rows with', error_count, 'errors'



class JSONParser:

    def __init__(self, obj, fields):
        assert type(fields) is list

        self.json = self._parse(obj)
        self.fields = fields


    def _parse(self, obj):

        stream = io.StringIO(obj) if type(obj) in (unicode, str) else obj

        return json.load(stream)

    def get_val(self, json_dict, field, clean=True):
        
        val = json_dict.get(field, None)
        val = ' '.join(val) if type(val) is list else unicode(val)
        if field in ('subheadline', 'data'):
            _, val = tag_cleaner.clean(val)
            
        if field == 'maindocdate':
            val = convert_to_unix_time(val, 'json') 
            
        if clean:
            return clean_val(val)
        else:
            return val

    def make_row(self, json_dict, sep):
        
        return sep.join([self.get_val(json_dict, col) for col in self.fields])
    
    def to_csv(self, sep):

        if type(self.json) is dict:
            
            yield self.make_row(self.json, sep)
            
        elif type(self.json) is list:
            
            for json_dict in self.json:
                
                yield self.make_row(json_dict, sep)
                
        else:
            raise Exception('JSON file must contain either a JSON object or list of JSON objects')


    def make_list(self, json_dict):

        return [self.get_val(json_dict, col, False) for col in self.fields]

    def gen_list(self):

        if type(self.json) is dict:
            
            yield self.make_list(self.json)
            
        elif type(self.json) is list:
            
            for json_dict in self.json:
                
                yield self.make_list(json_dict)
                
        else:
            raise Exception('JSON file must contain either a JSON object or list of JSON objects')



class JSONFile:

    def __init__(self, json_file_path):

        self.json_path = json_file_path
        self.fields = ['scrollid', 'sources', 'blank', 'maindocdate', 'blank', 'blank', 'hiorgid', 'tn2', 'subheadline', 'blank', 'data']

    def get_csv_header(self, sep):
        
        return sep.join(self.fields)

    def to_csv(self, csv_file, sep):

        write_buffer = list()

        rec_count = 0
        
        with io.open(self.json_path, mode="r", encoding='utf-8') as json_file:
        
            json_iter = JSONParser(json_file, self.fields).to_csv(sep)

            write_to_file = lambda s: csv_file.write(unicode(s))

            while True:

                try:
                    row = next(json_iter)
                    rec_count += 1
                    write_buffer.append(row)
                    
                except StopIteration:
                    break
                    
            write_to_file(u'\n'.join(write_buffer) + u'\n') 

            print 'Saved', rec_count, 'rows' 

    def to_df(self, columns):

        df = pd.DataFrame(columns=columns)
        rec_count = 0
        
        try:
            with io.open(self.json_path, mode="r", encoding='utf-8') as json_file:

                json_iter = JSONParser(json_file, self.fields).gen_list()

                while True:

                    try:
                        row = next(json_iter)
                        df.loc[rec_count] = row
                        rec_count += 1

                    except StopIteration:
                        break

                print 'Parsed', rec_count, 'rows' 

                return df
        except:
            print self.json_path

class CSVParser:
    
    def __init__(self, path):
        is_excel = path.split('.')[-1].lower() in ('xls', 'xlsx')
        
        if is_excel:
            self.df = pd.read_excel(path)
        else:
            self.df = pd.read_csv(path, encoding='utf-8')
        
        self.is_swift = 'SWIFT_' in path
        
        if self.is_swift:
            self.read_swift()
        else:
            self.read_nr()
        
    def to_df(self, columns):
        return self.df[columns]
        

    def delete_tags(self, text):
        _, clean_text = tag_cleaner.clean(text)
        return clean_text

    def to_unix_time(self, dt):
        file_type = 'json' if self.is_swift else 'xml'
        dt = dt if type(dt) in (str, unicode) else str(int(dt)) 
        return convert_to_unix_time(dt, file_type)
        
    def read_nr(self):
        column_names_map = {'story date time': 'story date', 'story title': 'title'}
        self.df.rename(columns=column_names_map, inplace=True)
        self.df['lead parag'] = ['' for _ in range(len(self.df))]
        self.df['title'] = self.df['title'].apply(self.delete_tags)
        self.df['story body'] = self.df['story body'].apply(self.delete_tags)     
        self.df['story date'] = self.df['story date'].apply(self.to_unix_time)
        
    def read_swift(self):
        column_names_map = {'scrollid': 'guid', 'sources': 'journal code', 'maindocdate': 'story date', 'hiorgid': 'RICs', 'tn2': 'RCS', 
                            'subheadline': 'title', 'data': 'story body'}
        blank_columns = ['source', 'person perm ids', 'OA perm ids', 'lead parag']
        blank_data = ['' for _ in range(len(self.df))]
        
        for col in blank_columns:
            self.df[col] = blank_data
            
        self.df.rename(columns=column_names_map, inplace=True)
        
        def unquote(val):
            return val.strip('"').strip().strip('"')
        
        def unlist(val):
            val = val.strip('[').strip(']')
            for rid in ('u', "'", ','):
                val = val.replace(rid, '') 
            return val
        
        for col in self.df.columns:
            self.df[col] = self.df[col].apply(unquote)
            if col in ('RICs', 'RCS'):
                self.df[col] = self.df[col].apply(unlist)
        
        self.df['title'] = self.df['title'].apply(self.delete_tags)
        self.df['story body'] = self.df['story body'].apply(self.delete_tags)     
        self.df['story date'] = self.df['story date'].apply(self.to_unix_time)
        

class NewsFile:

    def __init__(self, news_file_path):

        self.news_file_path = news_file_path
        self.extention = self.news_file_path.split('.')[-1].lower()

    def to_csv(self, csv_file, sep):

        if self.extention == 'xml':

            XMLFile(self.news_file_path).to_csv(csv_file, sep)

        elif self.extention in ('json', 'txt'):

            JSONFile(self.news_file_path).to_csv(csv_file, sep)

    def to_df(self, columns):

        if self.extention == 'xml':

            return XMLFile(self.news_file_path).to_df(columns)

        elif self.extention in ('json', 'txt'):

            return JSONFile(self.news_file_path).to_df(columns)

        elif self.extention in ('xls', 'xlsx', 'csv'):
            
            return CSVParser(self.news_file_path).to_df(columns)
        else:
            raise Exception('{}\nFile does not have the appropriate extension'.format(self.news_file_path))

class NewsData:

    def __init__(self, path):

        self.path = path
        self.columns = ['guid', 'journal code','source','story date','RICs','person perm ids','OA perm ids','RCS','title','lead parag','story body']
#         self.columns = ['guid','story date', 'journal code','OA perm ids','RCS','title','lead parag','story body']



    def to_df(self):

        if os.path.isfile(self.path):

            return NewsFile(self.path).to_df(self.columns)

        else:

            dfs = list()
            
            for file_name in os.listdir(self.path):

                file_path = os.path.join(self.path, file_name)

                if os.path.isfile(file_path):

                    dfs.append(NewsFile(file_path).to_df(self.columns))
                    
            return pd.concat(dfs, ignore_index=True)

    def to_csv(self, csv_file_path, sep):

        df = self.to_df()

        df['story body'] = df['lead parag'] + ' ' + df['story body']

        print 'Parsed total', len(df), 'rows'

        features = Features(df)
        features.add_sentiment()
        features.add_count_feats()
        features.add_pattern_counts()
        features.add_tfidf()
        features.clean()
        features.normalize_new_data()

        features.df.to_csv(path_or_buf=csv_file_path, sep=sep, encoding='utf-8', index=False, quoting=csv.QUOTE_ALL)


    def to_csv_as_is(self, csv_file_path, sep):

        with io.open(csv_file_path, mode="w", encoding='ascii', errors='ignore') as csv_file:

            csv_file.write(sep.join(self.columns) + u'\n')

            if os.path.isfile(self.path):

                NewsFile(self.path).to_csv(csv_file, sep)

            else:

                for file_name in os.listdir(self.path):

                    file_path = os.path.join(self.path, file_name)

                    if os.path.isfile(file_path):

                        NewsFile(file_path).to_csv(csv_file, sep)
                        
    def label_and_save(self, json_string, save_path, sep):
        objects = json.loads(json_string)
        guid_dict = defaultdict(str)
        for obj in objects:
            guid_dict[obj['guid']['guid']] = obj['significance']
        get_sig = lambda guid: guid_dict[guid]
        
        df = self.to_df()
        df['significance'] = df['guid'].apply(get_sig)
        df = df[['guid', 'significance']]
        df.to_csv(path_or_buf=save_path, sep=sep, encoding='utf-8', index=False, quoting=csv.QUOTE_ALL)