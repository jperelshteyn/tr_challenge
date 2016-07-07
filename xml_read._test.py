import codecs
from lxml import etree
import time


infile = 'content.xml'



def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        print '%s function took %0.3f ms' % (f.func_name, (time2-time1)*1000.0)
        return ret
    return wrap

@timing
def fast():
    c = etree.iterparse(infile, events=('end',), tag='n-document')
    with codecs.open('content1.txt', 'w+', 'utf8') as out:
        
        write_list = list()
        
        for event, elem in c:
            
            write_list.append(elem.xpath('string(n-docbody/document/content/text/p)'))
            #out.write(elem.xpath('string(n-docbody/document/content/text/p)'))

            if len(write_list) > 10:
                
                out.write(''.join(write_list))                
                
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
                
                write_list = list()

        out.write(''.join(write_list))
    


@timing            
def slow(): 
    c = etree.parse(infile).getroot()
    print len(c)
    with codecs.open('content2.txt', 'w+', 'utf8') as out:
        for elem in c:
            out.write(elem.xpath('string(n-docbody/document/content/text/p)'))
            
            
