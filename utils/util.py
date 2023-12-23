import os

from config import BASE_DIR

# ========================================================
#                   For printing tables
# ========================================================

def get_format_str(schema, max_length=10):
    format_str = ""
    for field in schema:
        format_str += f"{{:<{max_length}}}"
    return format_str

def print_table_header(schema, format_str):
    print("=" * len(format_str.format(*schema)))
    print(format_str.format(*schema))
    print("=" * len(format_str.format(*schema)))

# max_length must be >= 6
def print_row(row_dict, schema, format_str, max_length):
    row_list = []
    for field in schema:
        field_value = str(row_dict[field])
        field_value += "   "
        if len(field_value) > max_length:
            field_value = field_value[:max_length - 6] + "...   "
            row_list.append(field_value)
        else:
            row_list.append(field_value)
    print(format_str.format(*row_list))

# ========================================================
#                For mixed keys in NoSQL
# ========================================================

def mix_key(val):
        # numeric after string
        if type(val) == int or type(val) == float:
            return (1, val)
        else:
            return (0, val)
        
def get_key_val(key):
     return key[1]

def add_key(key1, key2):
    if key1[0] == key2[0]:
        return (key1[0], key1[1] + key2[1])
    else:
        rtn = 0
        if key1[0] == 1:
            rtn += key1[1]
        if key2[0] == 1:
            rtn += key2[1]
    return (1, rtn)

# ========================================================
#                   For the temp folder
# ========================================================

def clear_temp_files():
    for file in os.listdir(f"{BASE_DIR}/Temp"):
            # keep the .gitkeep file
            if file.endswith(".gitkeep") or file.endswith(".keep"):
                continue
            os.remove(f"{BASE_DIR}/Temp/{file}")