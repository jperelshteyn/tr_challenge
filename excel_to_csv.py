import pandas as pd
import os
import re
import sys

def clean_val(val):

    convert = lambda v: v.encode('ascii', 'replace')
    
    if type(val) in (unicode, str):
            
        for cr in ((u'\r', ' '), (u'\t', ' '), (u'\n', ' '), (u'"', "'")):
            val = val.replace(cr[0], cr[1])
            
        val = val.strip("'")
        
        if val.startswith('[') and val.endswith(']'):
            
            clean = lambda e: e[1:].strip("'") if e.startswith('u') else e.strip("'")
            
            val = ' '.join(map(clean, val[2:-2].split(', ')))
        
        return convert(val)
    else:
        return convert(unicode(val))


def parse_xls(file_name):

    df = pd.read_excel(file_name, encoding = 'utf8')
    col_dict = {'Significance Rating': 'Significance', 'Significance Rating ': 'Significance'}
    df.rename(columns=col_dict, inplace=True)
    
    for col in df.columns:

        if 'Unnamed:' in col:
            df.drop(col, axis=1, inplace=True)
            continue

        df[col] = df[col].apply(clean_val)
    
    return df

def get_df(path, file_cond):
    file_names = [path + f for f in os.listdir(path) if file_cond(f)]
    for fn in file_names:
        print 'Parsing', fn
        yield parse_xls(fn)
    
def save_csv(df, path, sep, line_end):
    
    df.to_csv(path_or_buf=path, sep=sep, encoding='utf-8', line_terminator=line_end, index=False)


def main():
    path = 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\excel\\'
    #path = 'D:\\thomson_reuters\\excel\\'
    sep = '\t'
    line_end = sep + '\n'
    file_conditions = {'swift': lambda f: os.path.isfile(path + f) and not f.startswith('NR_'),
                  'nsc': lambda f: f.startswith('NR_')}
    
    for file_name in file_conditions:
        
        count = 1
        df_gen = get_df(path, file_conditions[file_name])
        
        while count != 0:
            try:
                df = next(df_gen)
                print '\t', len(df), 'rows'
                csv_file_path = path + 'csv\\' + file_name + str(count) + '.txt'
                print 'Saving', csv_file_path
                save_csv(df, csv_file_path, sep, line_end)
                count += 1
            except StopIteration:
                count = 0

if __name__ == "__main__":
    main()  