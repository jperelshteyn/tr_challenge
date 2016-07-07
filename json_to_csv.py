import codecs
import json
import os
import io

def clean_val(val):
    val = ' '.join(val) if type(val) is list else unicode(val)
    for cr in (('\r', ' '), ('\t', ' '), ('\n', ' '), ('"', "'")):
        val = val.replace(cr[0], cr[1])
    return '"' +  val.encode('ascii', 'replace') + '"'

def get_val(json_obj, field):
    val = json_obj.get(field, 'NULL')
    return clean_val(val)

def get_files(path):
    for file_name in os.listdir(path):
        with io.open(path + file_name, mode='r', encoding='utf-8') as f:
            yield file_name, f


def parse_json(file_obj, fields):
    assert type(fields) is set
    
    json_obj = json.load(file_obj)
    for key in json_obj.keys():
        if key not in fields:
            del json_obj[key]
    return json_obj
    

def json_to_csv(json_obj, fields, sep, file_id):
    assert type(json_obj) is dict
    assert type(fields) is list
    assert type(sep) is str
    
    return sep.join([file_id] + [get_val(json_obj, col) for col in fields]) + '\t'


def save_data(data, file_path):
    with open(file_path, 'w+') as f:
        f.write(data)

def convert_files(file_limit=None):
    columns = ['fileID', 'scrollid', 'maindocdate', 'hiorgid', 'i2', 'if3', 'sources', 'subheadline', 'tn2', 'data']
    fields_list = ['scrollid', 'maindocdate', 'hiorgid', 'i2', 'if3', 'sources', 'subheadline', 'tn2', 'data']
    fields_set = set(fields_list)
    json_path = 'D:\\thomson_reuters\\swift\\json\\'
    csv_path = 'D:\\thomson_reuters\\swift\\csv\\'
    sep = '\t'

    files = get_files(json_path)
    data = [sep.join(columns)]
    counter = 1
    guids = list()
    
    while True:

        csv_file_path = csv_path + 'swift_csv_' + str(counter) + '.txt'

        try:
            file_name, json_file = next(files)
            file_id = file_name.split('.')[0]
            json_obj = parse_json(json_file, fields_set)
            csv_row = json_to_csv(json_obj, fields_list, sep, file_id)

            data.append(csv_row)
            guids.append(json_obj['scrollid'])

            if len(data) > 10000:
                save_data('\n'.join(data), csv_file_path)
                data = [sep.join(fields_list)]
                counter += 1
                if file_limit and counter > file_limit:
                    break

        except StopIteration:
            save_data('\n'.join(data), csv_file_path)
            save_data('\n'.join(guids), csv_path + 'parsed_guids.txt')
            break