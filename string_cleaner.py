import re


class StringCleaner:
    
    def __init__(self, delete_pattern=r'<[(A-Z)|(\.)]*>'):
         self.finder = re.compile(delete_pattern)
        
    
    def clean(self, string):
        changed = False 
        
        def match(tag):
            changed = True
            return ''
            
        clean_string = None
        try:
        	clean_string = self.finder.sub(match, string)
        except: 
        	print string
        return changed, clean_string