# for comparison in external sort
from utils.util import mix_key


class DocElement(object):
    # will be sorted using the field at field_index
    def __init__(self, chunk_num, doc, field, order_method):
        self.doc = doc
        self.field = field
        self.order_method = order_method
        self.chunk_num = chunk_num

    def __lt__(self, other):
        if self.order_method == 'asc':
            return mix_key(self.doc[self.field]) < mix_key(other.doc[self.field])
        else:
            return mix_key(self.doc[self.field]) > mix_key(other.doc[self.field])
        
    def __eq__(self, other):
        return mix_key(self.doc[self.field]) == mix_key(other.doc[self.field])
    
    def get_chunk_num(self):
        return self.chunk_num
    
    