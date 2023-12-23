# for comparison in external sort
class RowElement(object):
    # will be sorted using the field at field_index
    def __init__(self, chunk_num, row, field_index, order_method):
        self.chunk_num = chunk_num
        self.row = row
        self.field_index = field_index
        self.order_method = order_method

    def __lt__(self, other):
        if self.order_method == 'asc':
            return self.row[self.field_index] < other.row[self.field_index]
        else:
            return self.row[self.field_index] > other.row[self.field_index]
        
    def __eq__(self, other):
        return self.row[self.field_index] == other.row[self.field_index]
    
    def get_chunk_num(self):
        return self.chunk_num
    